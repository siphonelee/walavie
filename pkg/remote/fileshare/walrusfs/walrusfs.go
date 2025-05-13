// Copyright 2025, Command Line Inc.
// SPDX-License-Identifier: Apache-2.0

package walrusfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wavetermdev/waveterm/pkg/remote/connparse"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/fspath"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/fstype"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/fsutil"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/pathtree"
	"github.com/wavetermdev/waveterm/pkg/util/fileutil"
	"github.com/wavetermdev/waveterm/pkg/util/iochan/iochantypes"
	"github.com/wavetermdev/waveterm/pkg/util/tarcopy"
	"github.com/wavetermdev/waveterm/pkg/util/utilfn"
	"github.com/wavetermdev/waveterm/pkg/wconfig"
	"github.com/wavetermdev/waveterm/pkg/wshrpc"
	"github.com/wavetermdev/waveterm/pkg/wshutil"
)

type WalrusFsConfig struct {
	pkg           string
	root          string
	publisherUrl  string
	aggregatorUrl string
	mnemonic      string
	wallet        string
}

type WalrusClient struct {
	config *WalrusFsConfig
}

var _ fstype.FileShareClient = WalrusClient{}

func GetConfig() *WalrusFsConfig {
	fullConfig := wconfig.GetWatcher().GetFullConfig()

	var config WalrusFsConfig
	config.pkg = fullConfig.Settings.WalrusFsPackage
	config.root = fullConfig.Settings.WalrusFsRoot
	config.publisherUrl = fullConfig.Settings.WalrusFsPublisher
	config.aggregatorUrl = fullConfig.Settings.WalrusFsAggregator
	config.mnemonic = fullConfig.Settings.WalrusFsMnemonic
	config.wallet = fullConfig.Settings.WalrusFsWaallet

	return &config
}

func NewWalrusClient() *WalrusClient {
	return &WalrusClient{
		config: GetConfig(),
	}
}

func (c WalrusClient) Read(ctx context.Context, conn *connparse.Connection, data wshrpc.FileData) (*wshrpc.FileData, error) {
	rtnCh := c.ReadStream(ctx, conn, data)
	return fsutil.ReadStreamToFileData(ctx, rtnCh)
}

func (c WalrusClient) ReadStream(ctx context.Context, conn *connparse.Connection, data wshrpc.FileData) <-chan wshrpc.RespOrErrorUnion[wshrpc.FileData] {
	rtn := make(chan wshrpc.RespOrErrorUnion[wshrpc.FileData], 16)
	go func() {
		defer close(rtn)
		finfo, err := c.Stat(ctx, conn)
		if err != nil {
			rtn <- wshutil.RespErr[wshrpc.FileData](err)
			return
		} else if finfo.NotFound {
			rtn <- wshutil.RespErr[wshrpc.FileData](errors.New("no such file"))
			return
		}
		rtn <- wshrpc.RespOrErrorUnion[wshrpc.FileData]{Response: wshrpc.FileData{Info: finfo}}
		if finfo.NotFound {
			rtn <- wshrpc.RespOrErrorUnion[wshrpc.FileData]{Response: wshrpc.FileData{Entries: []*wshrpc.FileInfo{
				{
					Path:     finfo.Dir,
					Dir:      fspath.Dir(finfo.Dir),
					Name:     "..",
					IsDir:    true,
					Size:     0,
					ModTime:  time.Now().Unix(),
					MimeType: "directory",
				},
			}}}
			return
		}
		if finfo.IsDir {
			listEntriesCh := c.ListEntriesStream(ctx, conn, nil)
			defer func() {
				utilfn.DrainChannelSafe(listEntriesCh, "s3fs.ReadStream")
			}()
			for respUnion := range listEntriesCh {
				if respUnion.Error != nil {
					rtn <- wshutil.RespErr[wshrpc.FileData](respUnion.Error)
					return
				}
				resp := respUnion.Response
				if len(resp.FileInfo) > 0 {
					rtn <- wshrpc.RespOrErrorUnion[wshrpc.FileData]{Response: wshrpc.FileData{Entries: resp.FileInfo}}
				}
			}
		} else {
			if data.At != nil {
				log.Printf("reading %v with offset %d and size %d", conn.GetFullURI(), data.At.Offset, data.At.Size)
				rtn <- wshutil.RespErr[wshrpc.FileData](errors.New("can't read partial file"))
			}

			b, err := get_file(c.config, finfo.WalrusBlobId)
			if err != nil {
				rtn <- wshutil.RespErr[wshrpc.FileData](err)
				return
			}

			fullpath := conn.GetFullURI()
			finfo := &wshrpc.FileInfo{
				Name:    finfo.Name,
				IsDir:   false,
				Size:    finfo.Size,
				ModTime: finfo.ModTime,
				Path:    fullpath,
				Dir:     fsutil.GetParentPathString(fullpath),
			}
			fileutil.AddMimeTypeToFileInfo(finfo.Path, finfo)
			rtn <- wshrpc.RespOrErrorUnion[wshrpc.FileData]{Response: wshrpc.FileData{Info: finfo}}
			if finfo.Size == 0 {
				log.Printf("no data to read")
				return
			}

			rtn <- wshrpc.RespOrErrorUnion[wshrpc.FileData]{Response: wshrpc.FileData{Data64: base64.StdEncoding.EncodeToString(b)}}
		}
	}()
	return rtn
}

func (c WalrusClient) ReadTarStream(ctx context.Context, conn *connparse.Connection, opts *wshrpc.FileCopyOpts) <-chan wshrpc.RespOrErrorUnion[iochantypes.Packet] {
	recursive := opts != nil && opts.Recursive
	bucket := conn.Host
	if bucket == "" || bucket == "/" {
		return wshutil.SendErrCh[iochantypes.Packet](fmt.Errorf("bucket must be specified"))
	}

	// whether the operation is on the whole bucket
	wholeBucket := conn.Path == "" || conn.Path == fspath.Separator

	// get the object if it's a single file operation
	var singleFileResult *s3.GetObjectOutput
	// this ensures we don't leak the object if we error out before copying it
	closeSingleFileResult := true
	defer func() {
		// in case we error out before the object gets copied, make sure to close it
		if singleFileResult != nil && closeSingleFileResult {
			utilfn.GracefulClose(singleFileResult.Body, "s3fs", conn.Path)
		}
	}()
	if !wholeBucket {
	}

	// whether the operation is on a single file
	singleFile := singleFileResult != nil

	if !singleFile && !recursive {
		return wshutil.SendErrCh[iochantypes.Packet](fmt.Errorf(fstype.RecursiveRequiredError))
	}

	// whether to include the directory itself in the tar
	includeDir := (wholeBucket && conn.Path == "") || (singleFileResult == nil && conn.Path != "" && !strings.HasSuffix(conn.Path, fspath.Separator))

	timeout := fstype.DefaultTimeout
	if opts.Timeout > 0 {
		timeout = time.Duration(opts.Timeout) * time.Millisecond
	}
	readerCtx, cancel := context.WithTimeout(context.Background(), timeout)

	// the prefix that should be removed from the tar paths
	tarPathPrefix := conn.Path

	if wholeBucket {
		// we treat the bucket name as the root directory. If we're not including the directory itself, we need to remove the bucket name from the tar paths
		if includeDir {
			tarPathPrefix = ""
		} else {
			tarPathPrefix = bucket
		}
	} else if singleFile || includeDir {
		// if we're including the directory itself, we need to remove the last part of the path
		tarPathPrefix = fsutil.GetParentPathString(tarPathPrefix)
	}

	rtn, writeHeader, fileWriter, tarClose := tarcopy.TarCopySrc(readerCtx, tarPathPrefix)
	go func() {
		defer func() {
			tarClose()
			cancel()
		}()

		// below we get the objects concurrently so we need to store the results in a map
		objMap := make(map[string]*s3.GetObjectOutput)
		// close the objects when we're done
		defer func() {
			for key, obj := range objMap {
				utilfn.GracefulClose(obj.Body, "s3fs", key)
			}
		}()

		// tree to keep track of the paths we've added and insert fake directories for subpaths
		tree := pathtree.NewTree(tarPathPrefix, "/")

		if singleFile {
			objMap[conn.Path] = singleFileResult
			tree.Add(conn.Path)
		} else {
			// list the objects in the bucket and add them to a tree that we can then walk to write the tar entries
			var input *s3.ListObjectsV2Input
			if wholeBucket {
				// get all the objects in the bucket
				input = &s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
				}
			} else {
				objectPrefix := conn.Path
				if !strings.HasSuffix(objectPrefix, fspath.Separator) {
					objectPrefix = objectPrefix + fspath.Separator
				}
				input = &s3.ListObjectsV2Input{
					Bucket: aws.String(bucket),
					Prefix: aws.String(objectPrefix),
				}
			}

			errs := make([]error, 0)
			// wait group to await the finished fetches
			wg := sync.WaitGroup{}
			getObjectAndFileInfo := func(obj *ListDirFileItem) {
				defer wg.Done()
			}

			if err := c.listFilesPrefix(ctx, *input.Prefix, func(obj *ListDirFileItem) (bool, error) {
				wg.Add(1)
				go getObjectAndFileInfo(obj)
				return true, nil
			}); err != nil {
				rtn <- wshutil.RespErr[iochantypes.Packet](err)
				return
			}
			wg.Wait()
			if len(errs) > 0 {
				rtn <- wshutil.RespErr[iochantypes.Packet](errors.Join(errs...))
				return
			}
		}

		// Walk the tree and write the tar entries
		if err := tree.Walk(func(path string, numChildren int) error {
			mapEntry, isFile := objMap[path]

			// default vals assume entry is dir, since mapEntry might not exist
			modTime := int64(time.Now().Unix())
			mode := fstype.DirMode
			size := int64(numChildren)

			if isFile {
				mode = fstype.FileMode
				size = *mapEntry.ContentLength
				if mapEntry.LastModified != nil {
					modTime = mapEntry.LastModified.UnixMilli()
				}
			}

			finfo := &wshrpc.FileInfo{
				Name:    path,
				IsDir:   !isFile,
				Size:    size,
				ModTime: modTime,
				Mode:    mode,
			}
			if err := writeHeader(fileutil.ToFsFileInfo(finfo), path, singleFile); err != nil {
				return err
			}
			if isFile {
				if n, err := io.Copy(fileWriter, mapEntry.Body); err != nil {
					return err
				} else if n != size {
					return fmt.Errorf("error copying %v; expected to read %d bytes, but read %d", path, size, n)
				}
			}
			return nil
		}); err != nil {
			log.Printf("error walking tree: %v", err)
			rtn <- wshutil.RespErr[iochantypes.Packet](err)
			return
		}
	}()
	// we've handed singleFileResult off to the tar writer, so we don't want to close it
	closeSingleFileResult = false
	return rtn
}

func (c WalrusClient) ListEntries(ctx context.Context, conn *connparse.Connection, opts *wshrpc.FileListOpts) ([]*wshrpc.FileInfo, error) {
	var entries []*wshrpc.FileInfo
	rtnCh := c.ListEntriesStream(ctx, conn, opts)
	for respUnion := range rtnCh {
		if respUnion.Error != nil {
			return nil, respUnion.Error
		}
		resp := respUnion.Response
		entries = append(entries, resp.FileInfo...)
	}
	return entries, nil
}

func (c WalrusClient) ListEntriesStream(ctx context.Context, conn *connparse.Connection, opts *wshrpc.FileListOpts) <-chan wshrpc.RespOrErrorUnion[wshrpc.CommandRemoteListEntriesRtnData] {
	dirPrefix := conn.Path
	if dirPrefix != "" && !strings.HasSuffix(dirPrefix, fspath.Separator) {
		dirPrefix = dirPrefix + "/"
	}
	numToFetch := wshrpc.MaxDirSize
	if opts != nil && opts.Limit > 0 {
		numToFetch = min(opts.Limit, wshrpc.MaxDirSize)
	}
	numFetched := 0
	rtn := make(chan wshrpc.RespOrErrorUnion[wshrpc.CommandRemoteListEntriesRtnData], 16)
	// keep track of "directories" that have been used to avoid duplicates between pages
	prevUsedDirKeys := make(map[string]any)
	go func() {
		defer close(rtn)
		entryMap := make(map[string]*wshrpc.FileInfo)
		if err := c.listFilesPrefix(ctx, dirPrefix, func(item *ListDirFileItem) (bool, error) {
			if numFetched >= numToFetch {
				return false, nil
			}

			lastModTime := item.CreateTs

			// get the first level directory name or file name
			name, isDir := item.Name, item.IsDir
			// path := fspath.Join(conn.GetPathWithHost(), name)
			path := "walrus://" + conn.Path
			fullpath := ""
			if strings.HasPrefix(name, fspath.Separator) {
				fullpath = path + name
			} else {
				fullpath = path + fspath.Separator + name
			}
			if isDir {
				if entryMap[fullpath] == nil {
					if _, ok := prevUsedDirKeys[fullpath]; !ok {
						entryMap[fullpath] = &wshrpc.FileInfo{
							Path:    fullpath,
							Name:    name,
							IsDir:   true,
							Dir:     fsutil.GetParentPathString(fullpath),
							ModTime: lastModTime,
							Size:    0,
						}
						fileutil.AddMimeTypeToFileInfo(fullpath, entryMap[fullpath])

						prevUsedDirKeys[fullpath] = struct{}{}
						numFetched++
					}
				} else if entryMap[fullpath].ModTime < lastModTime {
					entryMap[fullpath].ModTime = lastModTime
				}
				return true, nil
			}

			size := item.Size
			entryMap[fullpath] = &wshrpc.FileInfo{
				Name:    name,
				IsDir:   false,
				Dir:     fsutil.GetParentPathString(fullpath),
				Path:    fullpath,
				ModTime: lastModTime,
				Size:    size,
			}
			fileutil.AddMimeTypeToFileInfo(fullpath, entryMap[fullpath])
			numFetched++
			return true, nil
		}); err != nil {
			rtn <- wshutil.RespErr[wshrpc.CommandRemoteListEntriesRtnData](err)
			return
		}
		entries := make([]*wshrpc.FileInfo, 0, wshrpc.DirChunkSize)
		for _, entry := range entryMap {
			entries = append(entries, entry)
			if len(entries) == wshrpc.DirChunkSize {
				rtn <- wshrpc.RespOrErrorUnion[wshrpc.CommandRemoteListEntriesRtnData]{Response: wshrpc.CommandRemoteListEntriesRtnData{FileInfo: entries}}
				entries = make([]*wshrpc.FileInfo, 0, wshrpc.DirChunkSize)
			}
		}
		if len(entries) > 0 {
			rtn <- wshrpc.RespOrErrorUnion[wshrpc.CommandRemoteListEntriesRtnData]{Response: wshrpc.CommandRemoteListEntriesRtnData{FileInfo: entries}}
		}
	}()
	return rtn
}

func (c WalrusClient) Stat(ctx context.Context, conn *connparse.Connection) (*wshrpc.FileInfo, error) {
	objectKey := conn.Path

	if objectKey == "" || objectKey == fspath.Separator {
		// root, refers to list all buckets
		return &wshrpc.FileInfo{
			Name:     fspath.Separator,
			IsDir:    true,
			Size:     0,
			ModTime:  0,
			Path:     "walrus://" + fspath.Separator,
			Dir:      "walrus://" + fspath.Separator,
			MimeType: "directory",
		}, nil
	}

	item, err := stat(c.config, conn.Path)
	if err != nil {
		return nil, err
	}
	if item == nil {
		// not found
		return &wshrpc.FileInfo{
			NotFound: true,
		}, nil
	}

	fullpath := "walrus://" + conn.Path
	fullpath = strings.TrimSuffix(fullpath, "/")

	// calvin
	rtn := &wshrpc.FileInfo{
		Name:         item.Name,
		Path:         fullpath,
		Dir:          fsutil.GetParentPathString(fullpath),
		IsDir:        item.IsDir,
		Size:         item.Size,
		ModTime:      item.CreateTs,
		WalrusBlobId: item.WalrusBlobId,
	}
	fileutil.AddMimeTypeToFileInfo(rtn.Path, rtn)
	return rtn, nil
}

func (c WalrusClient) PutFile(ctx context.Context, conn *connparse.Connection, data wshrpc.FileData) error {
	if data.At != nil {
		return errors.Join(errors.ErrUnsupported, fmt.Errorf("file data offset and size not supported"))
	}

	contentMaxLength := base64.StdEncoding.DecodedLen(len(data.Data64))
	var decodedBody []byte
	var contentLength int
	var err error
	if contentMaxLength > 0 {
		decodedBody = make([]byte, contentMaxLength)
		contentLength, err = base64.StdEncoding.Decode(decodedBody, []byte(data.Data64))
		if err != nil {
			return err
		}
	} else {
		decodedBody = []byte("\n")
		contentLength = 1
	}

	// Calvin TODO: overwrite anyway?
	err = add_file_content(c.config, bytes.NewReader(decodedBody), int64(contentLength), conn.Path, true)
	return err
}

func (c WalrusClient) AppendFile(ctx context.Context, conn *connparse.Connection, data wshrpc.FileData) error {
	return errors.Join(errors.ErrUnsupported, fmt.Errorf("append file not supported"))
}

func (c WalrusClient) Mkdir(ctx context.Context, conn *connparse.Connection) error {
	err := create_directory(c.config, conn.Path)
	return err
}

func (c WalrusClient) Mkfile(ctx context.Context, filepath string, dstpath string, overwrite bool) error {
	err := add_file(c.config, filepath, dstpath, overwrite)
	return err
}

func (c WalrusClient) MoveInternal(ctx context.Context, srcConn, destConn *connparse.Connection, opts *wshrpc.FileCopyOpts) error {
	// called when renaming file or dir
	if srcConn.Scheme != connparse.ConnectionTypeWalrus || destConn.Scheme != connparse.ConnectionTypeWalrus {
		return fmt.Errorf("source and destination must both be walrus")
	}

	fi, err := c.Stat(ctx, srcConn)
	if err != nil {
		return err
	}

	err = nil
	if fi.IsDir {
		err = rename(c.config, srcConn.Path, destConn.Path, true)
	} else {
		err = rename(c.config, srcConn.Path, destConn.Path, false)
	}

	return err
}

func (c WalrusClient) CopyRemote(ctx context.Context, srcConn, destConn *connparse.Connection, srcClient fstype.FileShareClient, opts *wshrpc.FileCopyOpts) (bool, error) {
	if srcConn.Scheme == connparse.ConnectionTypeS3 && destConn.Scheme == connparse.ConnectionTypeS3 {
		return c.CopyInternal(ctx, srcConn, destConn, opts)
	}
	destBucket := destConn.Host
	if destBucket == "" || destBucket == fspath.Separator {
		return false, fmt.Errorf("destination bucket must be specified")
	}
	return fsutil.PrefixCopyRemote(ctx, srcConn, destConn, srcClient, c, func(bucket, path string, size int64, reader io.Reader) error {
		return nil
	}, opts)
}

func (c WalrusClient) CopyRecursive(basePath string, newDir string, currentDirObj string, res *DirAllResult) (bool, error) {
	// already exists?
	_, err := os.Open(basePath + fspath.Separator + newDir)
	if !os.IsNotExist(err) {
		return false, fmt.Errorf("destination path already exists")
	}

	basePath = basePath + fspath.Separator + newDir
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		return false, err
	}

	// file
	item := res.Dirs[currentDirObj]
	for fname, fid := range item.ChildrenFiles {
		filename := basePath + fspath.Separator + fname
		b, err := get_file(c.config, res.Files[fid].WalrusBlobId)
		if err != nil {
			return false, fmt.Errorf("failed to get walrus blob " + res.Files[fid].WalrusBlobId)
		}
		err = os.WriteFile(filename, b, 0644)
		if err != nil {
			return false, fmt.Errorf("failed to write walrus blob to " + filename)
		}
	}

	// sub-dir
	for dname, did := range item.ChildrenDirectories {
		b, err := c.CopyRecursive(basePath, dname, did, res)
		if err != nil {
			return b, err
		}
	}

	return true, nil
}

func (c WalrusClient) CopyInternal(ctx context.Context, srcConn, destConn *connparse.Connection, opts *wshrpc.FileCopyOpts) (bool, error) {
	if destConn.Scheme == "wsh" && destConn.Host == "local" {
		// walrus -> local
		fi, err := c.Stat(ctx, srcConn)
		if err != nil {
			return false, err
		}

		destPath, err := fileutil.FixPath(destConn.Path)
		if err != nil {
			return false, err
		}

		if fi.IsDir {
			res, err := get_dir_all(c.config, srcConn.Path)
			if err != nil {
				return false, err
			}

			newDir := fsutil.GetEndingPart(srcConn.Path)

			return c.CopyRecursive(destPath, newDir, res.Dirobj, res)
		} else {
			filename := fsutil.GetEndingPart(srcConn.Path)
			_, err := os.Open(destPath + fspath.Separator + filename)
			if !os.IsNotExist(err) {
				return false, fmt.Errorf("destination path already exists")
			}

			destname := destPath + fspath.Separator + filename
			b, err := get_file(c.config, fi.WalrusBlobId)
			if err != nil {
				return false, fmt.Errorf("failed to get walrus blob " + fi.WalrusBlobId)
			}
			err = os.WriteFile(destname, b, 0644)
			if err != nil {
				return false, fmt.Errorf("failed to write walrus blob to " + filename)
			}

			return true, nil
		}
	}

	return false, fmt.Errorf("src/destination not supported")
}

func (c WalrusClient) Delete(ctx context.Context, conn *connparse.Connection, recursive bool) error {
	var err error
	path := conn.Path
	path = strings.TrimSuffix(path, "/")
	log.Printf("Deleting objects with prefix %v", path)

	fi, err := c.Stat(ctx, conn)
	if err != nil {
		return err
	}

	if fi.IsDir {
		err = delete(c.config, path, true)
	} else {
		err = delete(c.config, path, false)
	}

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func (c WalrusClient) listFilesPrefix(ctx context.Context, dirPath string, fileCallback func(*ListDirFileItem) (bool, error)) error {
	items, err := list_directory(c.config, dirPath)
	if err != nil {
		return err
	}

	for _, item := range items {
		if cont, err := fileCallback(&item); err != nil {
			return err
		} else if !cont {
			return nil
		}
	}

	return nil
}

func (c WalrusClient) Join(ctx context.Context, conn *connparse.Connection, parts ...string) (*wshrpc.FileInfo, error) {
	var joinParts []string
	if conn.Path == "" || conn.Path == fspath.Separator {
		joinParts = parts
	} else {
		joinParts = append([]string{conn.Path}, parts...)
	}

	conn.Path = fspath.Join(joinParts...)
	return c.Stat(ctx, conn)
}

func (c WalrusClient) GetConnectionType() string {
	return connparse.ConnectionTypeWalrus
}

func (c WalrusClient) GetCapability() wshrpc.FileShareCapability {
	return wshrpc.FileShareCapability{
		CanAppend: false,
		CanMkdir:  true,
	}
}
