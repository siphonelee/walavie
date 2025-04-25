package fileop

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/wavetermdev/waveterm/pkg/remote/connparse"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/fstype"
	"github.com/wavetermdev/waveterm/pkg/remote/fileshare/walrusfs"
)

func copyDirToWalrus(walrus *walrusfs.WalrusClient, destpath string, finfo fs.FileInfo, srcFile string) error {
	conn := &connparse.Connection{Scheme: "walrus", Host: "local", Path: destpath}
	nextinfo, err := walrus.Stat(context.Background(), conn)
	if err != nil {
		return fmt.Errorf("cannot stat %q: %w", destpath, err)
	}
	if nextinfo.NotFound {
		// try creating the dir
		err = walrus.Mkdir(context.Background(), conn)
		if err != nil {
			return fmt.Errorf("cannot mkdir %q: %w", destpath, err)
		}
	}

	return nil
}

func copyFileToWalrus(walrus *walrusfs.WalrusClient, destpath string, finfo fs.FileInfo, srcFile string, overwrite bool) error {
	conn := &connparse.Connection{Scheme: "walrus", Host: "local", Path: destpath}
	nextinfo, err := walrus.Stat(context.Background(), conn)
	if err != nil {
		return fmt.Errorf("cannot stat %q: %w", destpath, err)
	}
	/*
		else if nextinfo.NotFound && !finfo.IsDir() {
			// file copy to existing dir - parent folder not existing
			return 0, fmt.Errorf("path error")
		}
	*/

	if nextinfo != nil {
		if nextinfo.IsDir {
			// file copy to existing dir
			// try to create file in directory
			destpath = filepath.Join(destpath, filepath.Base(finfo.Name()))
			conn.Path = destpath
			newdestinfo, err := walrus.Stat(context.Background(), conn)
			if err != nil {
				return fmt.Errorf("cannot stat file %q: %w", destpath, err)
			}
			if !newdestinfo.NotFound && !overwrite {
				return fmt.Errorf(fstype.OverwriteRequiredError, destpath)
			}
		} else {
			// file copy
			if !nextinfo.NotFound {
				if !overwrite {
					return fmt.Errorf(fstype.OverwriteRequiredError, destpath)
				}
			}
		}
	}

	err = walrus.Mkfile(context.Background(), srcFile, conn.Path, overwrite)
	if err != nil {
		return fmt.Errorf("cannot create walrus file %q: %w", destpath, err)
	}

	return nil
}

// func CopyLocalToWalrus() {
// 	walrus := walrusfs.NewWalrusClient()

// 	srcPathCleaned := filepath.Clean(wavebase.ExpandHomeDirSafe(srcConn.Path))

// 	srcFileStat, err := os.Stat(srcPathCleaned)
// 	if err != nil {
// 		return false, fmt.Errorf("cannot stat file %q: %w", srcPathCleaned, err)
// 	}

// 	fi, err := walrus.Stat(ctx, &connparse.Connection{Scheme: "walrus", Host: "local", Path: destConn.Path})
// 	if err != nil {
// 		return false, fmt.Errorf("cannot stat walrus %q: %w", destConn.Path, err)
// 	}
// 	destIsDir = fi.IsDir

// 	if srcFileStat.IsDir() {
// 		srcIsDir = true
// 		var srcPathPrefix string
// 		if destIsDir {
// 			srcPathPrefix = filepath.Dir(srcPathCleaned)
// 		} else {
// 			srcPathPrefix = srcPathCleaned
// 		}
// 		err = filepath.Walk(srcPathCleaned, func(path string, info fs.FileInfo, err error) error {
// 			if err != nil {
// 				return err
// 			}
// 			srcFilePath := path
// 			destFilePath := filepath.Join(destPathCleaned, strings.TrimPrefix(path, srcPathPrefix))
// 			var file *os.File
// 			if !info.IsDir() {
// 				file, err = os.Open(srcFilePath)
// 				if err != nil {
// 					return fmt.Errorf("cannot open file %q: %w", srcFilePath, err)
// 				}
// 				defer utilfn.GracefulClose(file, "RemoteFileCopyCommand", srcFilePath)
// 			}

// 			if info.IsDir() {
// 				_, err = copyDirToWalrus(walrus, destFilePath, info, srcFilePath)
// 			} else {
// 				_, err = copyFileToWalrus(walrus, destFilePath, info, srcFilePath)
// 			}
// 			return err
// 		})
// 		if err != nil {
// 			return false, fmt.Errorf("cannot copy %q to %q: %w", srcUri, destUri, err)
// 		}
// 	} else {
// 		// local file -> walrus

// 		file, err := os.Open(srcPathCleaned)
// 		if err != nil {
// 			return false, fmt.Errorf("cannot open file %q: %w", srcPathCleaned, err)
// 		}
// 		defer utilfn.GracefulClose(file, "RemoteFileCopyCommand", srcPathCleaned)
// 		/*
// 			var destFilePath string
// 			if destHasSlash {
// 				destFilePath = filepath.Join(destPathCleaned, filepath.Base(srcPathCleaned))
// 			} else {
// 				destFilePath = destPathCleaned
// 			}
// 		*/
// 		destFilePath := destPathCleaned
// 		_, err = copyFileToWalrus(walrus, destFilePath, srcFileStat, srcPathCleaned)
// 		if err != nil {
// 			return false, fmt.Errorf("cannot copy %q to %q: %w", srcUri, destUri, err)
// 		}
// 	}
// }

func CopyWalrusToLocal(srcpath string, destpath string) error {
	walrus := walrusfs.NewWalrusClient()

	src := &connparse.Connection{Scheme: "walrus", Host: "local", Path: srcpath}
	dst := &connparse.Connection{Scheme: "wsh", Host: "local", Path: destpath}

	_, err := walrus.CopyInternal(context.Background(), src, dst, nil)
	return err
}

func FileOperation(s string) (string, error) {
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")

	var jsonMap map[string]interface{}
	err := json.Unmarshal([]byte(s), &jsonMap)
	if err != nil {
		return "", err
	}

	src := jsonMap["src"].(string)
	dst := jsonMap["dst"].(string)

	switch jsonMap["operation"] {
	case "copy":
		if strings.HasPrefix(src, "walrus://") && !strings.HasPrefix(dst, "walrus://") {
			srcCleaned := strings.TrimPrefix(src, "walrus://")
			if !strings.HasPrefix(srcCleaned, "/") {
				srcCleaned = "/" + srcCleaned
			}
			err = CopyWalrusToLocal(srcCleaned, dst)
		} else if strings.HasPrefix(dst, "walrus://") && !strings.HasPrefix(src, "walrus://") {

		} else if !strings.HasPrefix(dst, "walrus://") && !strings.HasPrefix(src, "walrus://") {

		} else {
			return "", fmt.Errorf("unsupported file operation from %q to %q", src, dst)
		}
	}

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("successfully copied from %q to %q", src, dst), nil
}
