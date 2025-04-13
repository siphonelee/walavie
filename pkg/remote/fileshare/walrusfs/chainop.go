// Copyright 2025, Command Line Inc.
// SPDX-License-Identifier: Apache-2.0

package walrusfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/block-vision/sui-go-sdk/constant"
	"github.com/block-vision/sui-go-sdk/models"
	"github.com/block-vision/sui-go-sdk/signer"
	"github.com/block-vision/sui-go-sdk/sui"
)

type ListDirFileItem struct {
	CreateTs        int64    `json:"create_ts,int64"`
	IsDir           bool     `json:"is_dir,boolean"`
	Name            string   `json:"name,string"`
	Size            int64    `json:"size,int64"`
	Tags            []string `json:"tags"`
	WalrusBlobId    string   `json:"walrus_blob_id,string"`
	WalrusEpochTill int64    `json:"walrus_epoch_till,int64"`
}

type DirItem struct {
	ChildrenDirectories map[string]string `json:"children_directories"`
	ChildrenFiles       map[string]string `json:"children_files"`
	CreateTs            int64             `json:"create_ts,int64"`
	Tags                []string          `json:"tags"`
}

type ListDirResult struct {
	List []ListDirFileItem `json:"ListDirItem"`
}

type DirAllResult struct {
	Dirobj string                     `json:"dirobj"`
	Files  map[string]ListDirFileItem `json:"files"`
	Dirs   map[string]DirItem         `json:"dirs"`
}

func parse_dir_file_item(m map[string]interface{}) (error, ListDirFileItem) {
	var r ListDirFileItem

	i, err := strconv.ParseInt(m["create_ts"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.CreateTs = i

	r.IsDir = m["is_dir"].(bool)
	r.Name = m["name"].(string)

	i, err = strconv.ParseInt(m["size"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.Size = i

	r.Tags = *new([]string)
	for _, t := range m["tags"].([]interface{}) {
		r.Tags = append(r.Tags, t.(string))
	}

	r.WalrusBlobId = m["walrus_blob_id"].(string)

	i, err = strconv.ParseInt(m["walrus_epoch_till"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.WalrusEpochTill = i

	return nil, r
}

func parse_file_info(m map[string]interface{}) (error, ListDirFileItem) {
	var r ListDirFileItem

	i, err := strconv.ParseInt(m["create_ts"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.CreateTs = i

	r.IsDir = false

	i, err = strconv.ParseInt(m["size"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.Size = i

	r.Tags = *new([]string)
	for _, t := range m["tags"].([]interface{}) {
		r.Tags = append(r.Tags, t.(string))
	}

	r.WalrusBlobId = m["walrus_blob_id"].(string)

	i, err = strconv.ParseInt(m["walrus_epoch_till"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, ListDirFileItem{}
	}
	r.WalrusEpochTill = i

	return nil, r
}

func parse_dir_info(m map[string]interface{}) (error, DirItem) {
	r := DirItem{
		ChildrenDirectories: make(map[string]string),
		ChildrenFiles:       make(map[string]string),
		CreateTs:            0,
		Tags:                make([]string, 0),
	}

	cd := m["children_directories"].(map[string]interface{})["contents"].([]interface{})
	for _, cdi := range cd {
		cdim := cdi.(map[string]interface{})
		r.ChildrenDirectories[cdim["key"].(string)] = cdim["value"].(string)
	}

	cf := m["children_files"].(map[string]interface{})["contents"].([]interface{})
	for _, cfi := range cf {
		cfim := cfi.(map[string]interface{})
		r.ChildrenFiles[cfim["key"].(string)] = cfim["value"].(string)
	}

	i, err := strconv.ParseInt(m["create_ts"].(string), 10, 64)
	if err != nil {
		log.Printf("conversion error: %v", err)
		return err, r
	}
	r.CreateTs = i

	for _, t := range m["tags"].([]interface{}) {
		r.Tags = append(r.Tags, t.(string))
	}

	return nil, r
}

func parse_dir_all(m map[string]interface{}) (DirAllResult, error) {
	r := DirAllResult{
		Dirobj: "",
		Files:  make(map[string]ListDirFileItem),
		Dirs:   make(map[string]DirItem),
	}
	r.Dirobj = m["dirobj"].(string)

	files := m["files"].(map[string]interface{})["contents"].([]interface{})
	for _, f := range files {
		fm := f.(map[string]interface{})

		err, item := parse_file_info(fm["value"].(map[string]interface{}))
		if err != nil {
			return r, err
		}
		r.Files[fm["key"].(string)] = item
	}

	dirs := m["dirs"].(map[string]interface{})["contents"].([]interface{})
	for _, d := range dirs {
		dm := d.(map[string]interface{})
		err, item := parse_dir_info(dm["value"].(map[string]interface{}))
		if err != nil {
			return r, err
		}

		r.Dirs[dm["key"].(string)] = item
	}
	return r, nil
}

func stat(config *WalrusFsConfig, path string) (*ListDirFileItem, error) {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        "stat",
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			path,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return nil, err
	}

	rsp2, err := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return nil, err
	}

	rsp3, err := cli.SuiGetEvents(ctx, models.SuiGetEventsRequest{
		Digest: rsp2.Digest,
	})

	if err != nil {
		log.Printf("error SuiGetEvents: %v", err)
		return nil, err
	}

	if len(rsp3) == 0 {
		return nil, nil
	}

	item := rsp3[0].ParsedJson
	err, ret := parse_dir_file_item(item)
	if err != nil {
		return nil, err
	}

	return &ret, nil
}

func list_directory(config *WalrusFsConfig, path string) (error, []ListDirFileItem) {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return err, nil
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        "list_dir",
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			path,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return err, nil
	}

	rsp2, err := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return err, nil
	}

	rsp3, err := cli.SuiGetEvents(ctx, models.SuiGetEventsRequest{
		Digest: rsp2.Digest,
	})

	if err != nil {
		log.Printf("error SuiGetEvents: %v", err)
		return err, nil
	}

	var result []ListDirFileItem

	for _, item := range rsp3[0].ParsedJson["list"].([]interface{}) {
		var r ListDirFileItem

		m := item.(map[string]interface{})

		err, r := parse_dir_file_item(m)
		if err != nil {
			return err, nil
		}

		result = append(result, r)
	}

	return nil, result
}

func create_directory(config *WalrusFsConfig, path string) error {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	tags := make([]string, 0)
	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        "add_dir",
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			"0x6",
			path,
			tags,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return err
	}

	rsp2, err := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return err
	}

	_, err = cli.SuiGetEvents(ctx, models.SuiGetEventsRequest{
		Digest: rsp2.Digest,
	})

	if err != nil {
		log.Printf("error SuiGetEvents: %v", err)
		return err
	}

	return nil
}

func add_file_content(config *WalrusFsConfig, data io.Reader, len int64, dstpath string, overwrite bool) error {
	req, err := http.NewRequest("PUT", config.publisherUrl+"/v1/blobs?epochs=5", data)
	if err != nil {
		log.Printf("error http.NewRequest: %v", err)
		return err
	}

	httpclient := &http.Client{}
	res, err := httpclient.Do(req)
	if err != nil {
		log.Printf("error httpclient.Do: %v", err)
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Printf("error io.ReadAll: %v", err)
		return err
	}
	log.Println(string(body))

	var objmap map[string]interface{}
	if err := json.Unmarshal(body, &objmap); err != nil {
		log.Printf("error json.Unmarshal: %v", err)
		return err
	}

	blob_id := ""
	if objmap["newlyCreated"] != nil {
		nc := objmap["newlyCreated"].(map[string]interface{})
		bo := nc["blobObject"].(map[string]interface{})
		blob_id = bo["id"].(string)
	} else if objmap["alreadyCertified"] != nil {
		ac := objmap["alreadyCertified"].(map[string]interface{})
		blob_id = ac["blobId"].(string)
	} else {
		log.Printf("json error with no blob_id: %v", objmap)
		return err
	}

	// save info to sui
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	tags := make([]string, 0)
	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        "add_file",
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			"0x6",
			dstpath,
			tags,
			strconv.FormatInt(len, 10),
			blob_id,
			// calvin: TODO
			strconv.FormatInt(0, 10),
			overwrite,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return err
	}

	rsp2, err := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return err
	}

	_, err = cli.SuiGetEvents(ctx, models.SuiGetEventsRequest{
		Digest: rsp2.Digest,
	})

	if err != nil {
		log.Printf("error SuiGetEvents: %v", err)
		return err
	}

	return nil
}

func add_file(config *WalrusFsConfig, filepath string, dstpath string, overwrite bool) error {
	// publish to walrus
	data, err := os.Open(filepath)
	if err != nil {
		log.Printf("error Open file: %v", err)
		return err
	}
	defer data.Close()

	fi, err := data.Stat()
	if err != nil {
		log.Printf("error file Stat: %v", err)
		return err
	}

	return add_file_content(config, data, fi.Size(), dstpath, overwrite)
}

func get_file(config *WalrusFsConfig, blobId string) ([]byte, error) {
	resp, err := http.Get(config.aggregatorUrl + "/v1/blobs/" + blobId)
	if err != nil {
		log.Printf("error http.Get: %v", err)
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error ioutil.ReadAll: %v", err)
		return nil, err
	}

	return body, nil
}

func rename(config *WalrusFsConfig, frompath string, topath string, isdir bool) error {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	var funcname string
	if isdir {
		funcname = "rename_dir"
	} else {
		funcname = "rename_file"
	}
	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        funcname,
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			frompath,
			topath,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return err
	}

	_, err = cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return err
	}

	return nil
}

func delete(config *WalrusFsConfig, path string, isdir bool) error {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	var funcname string
	if isdir {
		funcname = "delete_dir"
	} else {
		funcname = "delete_file"
	}
	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        funcname,
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			path,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return err
	}

	_, err = cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return err
	}

	return nil
}

func get_dir_all(config *WalrusFsConfig, path string) (*DirAllResult, error) {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	priKey := signerAccount.PriKey
	var ctx = context.Background()

	rsp, err := cli.MoveCall(ctx, models.MoveCallRequest{
		Signer:          signerAccount.Address,
		PackageObjectId: config.pkg,
		Module:          "walrusfs",
		Function:        "get_dir_all",
		TypeArguments:   []interface{}{},
		Arguments: []interface{}{
			config.root,
			path,
		},
		GasBudget: "100000000",
	})

	if err != nil {
		log.Printf("error MoveCall: %v", err)
		return nil, err
	}

	rsp2, err := cli.SignAndExecuteTransactionBlock(ctx, models.SignAndExecuteTransactionBlockRequest{
		TxnMetaData: rsp,
		PriKey:      priKey,
		// only fetch the effects field
		Options: models.SuiTransactionBlockOptions{
			ShowInput:    true,
			ShowRawInput: true,
			ShowEffects:  true,
		},
		RequestType: "WaitForLocalExecution",
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return nil, err
	}

	rsp3, err := cli.SuiGetEvents(ctx, models.SuiGetEventsRequest{
		Digest: rsp2.Digest,
	})

	if err != nil {
		log.Printf("error SuiGetEvents: %v", err)
		return nil, err
	}

	if len(rsp3) == 0 {
		return nil, nil
	}

	ret := rsp3[0].ParsedJson
	res, err := parse_dir_all(ret)
	if err != nil {
		return nil, err
	}

	return &res, nil
}
