// Copyright 2025, Command Line Inc.
// SPDX-License-Identifier: Apache-2.0

package walrusfs

import (
	"bytes"
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
	"github.com/block-vision/sui-go-sdk/mystenbcs"
	"github.com/block-vision/sui-go-sdk/signer"
	"github.com/block-vision/sui-go-sdk/sui"
	"github.com/block-vision/sui-go-sdk/transaction"
	"github.com/fardream/go-bcs/bcs"
	"github.com/holiman/uint256"
)

type ListDirFileItem struct {
	Name            string   `json:"name,string"`
	CreateTs        int64    `json:"create_ts,int64"`
	IsDir           bool     `json:"is_dir,boolean"`
	Tags            []string `json:"tags"`
	Size            int64    `json:"size,int64"`
	WalrusBlobId    string   `json:"walrus_blob_id,string"`
	WalrusEpochTill int64    `json:"walrus_epoch_till,int64"`
}

type DirItem struct {
	CreateTs            int64             `json:"create_ts,int64"`
	Tags                []string          `json:"tags"`
	ChildrenFiles       map[string]string `json:"children_files"`
	ChildrenDirectories map[string]string `json:"children_directories"`
}

type ListDirResult struct {
	List []ListDirFileItem `json:"ListDirItem"`
}

type DirAllResult struct {
	Dirobj string                     `json:"dirobj"`
	Files  map[string]ListDirFileItem `json:"files"`
	Dirs   map[string]DirItem         `json:"dirs"`
}

type FileObject struct {
	CreateTs        int64
	Tags            []string
	Size            uint64
	WalrusBlobId    string
	WalrusEpochTill uint64
}

type DirObject struct {
	CreateTs            int64                  `json:"create_ts,int64"`
	Tags                []string               `json:"tags"`
	ChildrenFiles       map[string]uint256.Int `json:"children_files"`
	ChildrenDirectories map[string]uint256.Int `json:"children_directories"`
}

type FileObjectEx struct {
	Id  uint256.Int
	Obj FileObject
}

type DirObjectEx struct {
	Id                     uint256.Int
	CreateTs               uint64
	Tags                   []string
	ChildrenFileNames      []string
	ChildrenFileIds        []uint256.Int
	ChildrenDirectoryNames []string
	ChildrenDirectoryIds   []uint256.Int
}

type RecursiveDirList struct {
	Dirobj uint256.Int
	Files  []FileObjectEx
	Dirs   []DirObjectEx
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

func parse_file_info(f *FileObjectEx) (error, uint256.Int, ListDirFileItem) {
	var r ListDirFileItem

	r.CreateTs = f.Obj.CreateTs
	r.IsDir = false
	r.Size = int64(f.Obj.Size)
	r.Tags = f.Obj.Tags
	r.WalrusBlobId = f.Obj.WalrusBlobId
	r.WalrusEpochTill = int64(f.Obj.WalrusEpochTill)

	return nil, f.Id, r
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

func parse_dir_all(list *RecursiveDirList) (DirAllResult, error) {
	r := DirAllResult{
		Dirobj: "",
		Files:  make(map[string]ListDirFileItem),
		Dirs:   make(map[string]DirItem),
	}
	r.Dirobj = list.Dirobj.String()

	for _, d := range list.Dirs {
		item := DirItem{
			ChildrenFiles:       make(map[string]string),
			ChildrenDirectories: make(map[string]string),
		}

		item.Tags = d.Tags
		item.CreateTs = int64(d.CreateTs)

		sz := len(d.ChildrenFileIds)
		for i := 0; i < sz; i++ {
			item.ChildrenFiles[d.ChildrenFileNames[i]] = d.ChildrenFileIds[i].String()
		}

		sz = len(d.ChildrenDirectoryIds)
		for i := 0; i < sz; i++ {
			item.ChildrenDirectories[d.ChildrenDirectoryNames[i]] = d.ChildrenDirectoryIds[i].String()
		}

		r.Dirs[d.Id.String()] = item
	}

	for _, f := range list.Files {
		err, key, item := parse_file_info(&f)
		if err != nil {
			return r, err
		}
		r.Files[key.String()] = item
	}

	return r, nil
}

func stat(config *WalrusFsConfig, path string) (*ListDirFileItem, error) {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)
	ctx := context.Background()

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rsp, err := cli.SuiGetObject(ctx, models.SuiGetObjectRequest{
		ObjectId: config.root,
		Options: models.SuiObjectDataOptions{
			ShowContent:             false,
			ShowDisplay:             false,
			ShowType:                false,
			ShowBcs:                 false,
			ShowOwner:               false,
			ShowPreviousTransaction: false,
			ShowStorageRebate:       false,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to SuiGetObject: %w", err)
	}

	ver, err := strconv.ParseUint(rsp.Data.Version, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to ParseUint: %w", err)
	}

	objectIdBytes, err := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(config.root))
	if err != nil {
		return nil, fmt.Errorf("failed to convert address: %w", err)
	}

	digestBytes, err := transaction.ConvertObjectDigestStringToBytes((models.ObjectDigest)(rsp.Data.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to convert digest: %w", err)
	}

	tx := transaction.NewTransaction()

	bcsEncodedMsg := bytes.Buffer{}
	bcsEncoder := mystenbcs.NewEncoder(&bcsEncodedMsg)
	err = bcsEncoder.Encode(path)
	if err != nil {
		return nil, fmt.Errorf("failed to Encode param: %w", err)
	}
	val := bcsEncodedMsg.Bytes()
	arg := tx.Data.V1.AddInput(transaction.CallArg{Pure: &transaction.Pure{
		Bytes: val,
	}})

	arguments := []transaction.Argument{
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					ImmOrOwnedObject: &transaction.SuiObjectRef{
						ObjectId: *objectIdBytes,
						Version:  ver,
						Digest:   *digestBytes,
					},
				},
			},
		),
		arg,
	}

	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(signerAccount.Address))
	tx.SetGasBudget(100000000)
	tx.MoveCall(
		models.SuiAddress(config.pkg),
		"walrusfs",
		"stat",
		[]transaction.TypeTag{},
		arguments,
	)

	encodedMsg, err := tx.Data.V1.Kind.Marshal()
	if err != nil {
		log.Printf("error tx.Data.V1.Kind.Marshal: %v", err)
		return nil, err
	}

	txBytes := mystenbcs.ToBase64(encodedMsg)

	// 5. Call SuiDevInspectTransactionBlock
	rsp2, err := cli.SuiDevInspectTransactionBlock(ctx, models.SuiDevInspectTransactionBlockRequest{
		Sender:  config.wallet,
		TxBytes: txBytes,
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return nil, err
	}
	if len(rsp2.Results) == 0 {
		// nothing returned, not found
		return nil, nil
	}

	type moveCallResult struct {
		ReturnValues [2]interface{}
	}

	var moveCallReturn []moveCallResult
	err = json.Unmarshal(rsp2.Results, &moveCallReturn)
	if err != nil {
		fmt.Println("json", err.Error())
		return nil, err
	}

	var dlo ListDirFileItem
	t := moveCallReturn[0].ReturnValues
	t1 := t[0].([]interface{})
	t2 := t1[0].([]interface{})
	output := make([]byte, 0, len(t2))
	for i := range t2 {
		bv := byte(int(t2[i].(float64)))
		output = append(output, bv)
	}

	if _, err := bcs.Unmarshal(output, &dlo); err != nil {
		log.Printf("failed to decode: %v", err.Error())
		return nil, err
	}

	return &dlo, nil
}

func list_directory(config *WalrusFsConfig, path string) ([]ListDirFileItem, error) {
	cli := sui.NewSuiClient(constant.SuiTestnetEndpoint)
	ctx := context.Background()

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rsp, err := cli.SuiGetObject(ctx, models.SuiGetObjectRequest{
		ObjectId: config.root,
		Options: models.SuiObjectDataOptions{
			ShowContent:             false,
			ShowDisplay:             false,
			ShowType:                false,
			ShowBcs:                 false,
			ShowOwner:               false,
			ShowPreviousTransaction: false,
			ShowStorageRebate:       false,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to SuiGetObject: %w", err)
	}

	ver, err := strconv.ParseUint(rsp.Data.Version, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to ParseUint: %w", err)
	}

	objectIdBytes, err := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(config.root))
	if err != nil {
		return nil, fmt.Errorf("failed to convert address: %w", err)
	}

	digestBytes, err := transaction.ConvertObjectDigestStringToBytes((models.ObjectDigest)(rsp.Data.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to convert digest: %w", err)
	}

	tx := transaction.NewTransaction()

	bcsEncodedMsg := bytes.Buffer{}
	bcsEncoder := mystenbcs.NewEncoder(&bcsEncodedMsg)
	err = bcsEncoder.Encode(path)
	if err != nil {
		return nil, fmt.Errorf("failed to Encode param: %w", err)
	}
	val := bcsEncodedMsg.Bytes()
	arg := tx.Data.V1.AddInput(transaction.CallArg{Pure: &transaction.Pure{
		Bytes: val,
	}})

	arguments := []transaction.Argument{
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					ImmOrOwnedObject: &transaction.SuiObjectRef{
						ObjectId: *objectIdBytes,
						Version:  ver,
						Digest:   *digestBytes,
					},
				},
			},
		),
		arg,
	}

	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(signerAccount.Address))
	tx.SetGasBudget(100000000)
	tx.MoveCall(
		models.SuiAddress(config.pkg),
		"walrusfs",
		"list_dir",
		[]transaction.TypeTag{},
		arguments,
	)

	encodedMsg, err := tx.Data.V1.Kind.Marshal()
	if err != nil {
		log.Printf("error tx.Data.V1.Kind.Marshal: %v", err)
		return nil, err
	}

	txBytes := mystenbcs.ToBase64(encodedMsg)

	rsp2, err := cli.SuiDevInspectTransactionBlock(ctx, models.SuiDevInspectTransactionBlockRequest{
		Sender:  config.wallet,
		TxBytes: txBytes,
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return nil, err
	}

	type moveCallResult struct {
		ReturnValues [2]interface{}
	}

	var moveCallReturn []moveCallResult
	err = json.Unmarshal(rsp2.Results, &moveCallReturn)
	if err != nil {
		fmt.Println("json", err.Error())
		return nil, err
	}

	var dlo []ListDirFileItem
	t := moveCallReturn[0].ReturnValues
	t1 := t[0].([]interface{})
	t2 := t1[0].([]interface{})
	output := make([]byte, 0, len(t2))
	for i := range t2 {
		bv := byte(int(t2[i].(float64)))
		output = append(output, bv)
	}

	if _, err := bcs.Unmarshal(output, &dlo); err != nil {
		log.Printf("failed to decode: %v", err.Error())
		return nil, err
	}

	return dlo, nil
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
		blob_id = bo["blobId"].(string)
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
	ctx := context.Background()

	signerAccount, err := signer.NewSignertWithMnemonic(config.mnemonic)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	rsp, err := cli.SuiGetObject(ctx, models.SuiGetObjectRequest{
		ObjectId: config.root,
		Options: models.SuiObjectDataOptions{
			ShowContent:             false,
			ShowDisplay:             false,
			ShowType:                false,
			ShowBcs:                 false,
			ShowOwner:               false,
			ShowPreviousTransaction: false,
			ShowStorageRebate:       false,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to SuiGetObject: %w", err)
	}

	ver, err := strconv.ParseUint(rsp.Data.Version, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to ParseUint: %w", err)
	}

	objectIdBytes, err := transaction.ConvertSuiAddressStringToBytes(models.SuiAddress(config.root))
	if err != nil {
		return nil, fmt.Errorf("failed to convert address: %w", err)
	}

	digestBytes, err := transaction.ConvertObjectDigestStringToBytes((models.ObjectDigest)(rsp.Data.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to convert digest: %w", err)
	}

	tx := transaction.NewTransaction()

	bcsEncodedMsg := bytes.Buffer{}
	bcsEncoder := mystenbcs.NewEncoder(&bcsEncodedMsg)
	err = bcsEncoder.Encode(path)
	if err != nil {
		return nil, fmt.Errorf("failed to Encode param: %w", err)
	}
	val := bcsEncodedMsg.Bytes()
	arg := tx.Data.V1.AddInput(transaction.CallArg{Pure: &transaction.Pure{
		Bytes: val,
	}})

	arguments := []transaction.Argument{
		tx.Object(
			transaction.CallArg{
				Object: &transaction.ObjectArg{
					ImmOrOwnedObject: &transaction.SuiObjectRef{
						ObjectId: *objectIdBytes,
						Version:  ver,
						Digest:   *digestBytes,
					},
				},
			},
		),
		arg,
	}

	tx.SetSuiClient(cli.(*sui.Client))
	tx.SetSender(models.SuiAddress(signerAccount.Address))
	tx.SetGasBudget(100000000)
	tx.MoveCall(
		models.SuiAddress(config.pkg),
		"walrusfs",
		"get_dir_all",
		[]transaction.TypeTag{},
		arguments,
	)

	encodedMsg, err := tx.Data.V1.Kind.Marshal()
	if err != nil {
		log.Printf("error tx.Data.V1.Kind.Marshal: %v", err)
		return nil, err
	}

	txBytes := mystenbcs.ToBase64(encodedMsg)

	rsp2, err := cli.SuiDevInspectTransactionBlock(ctx, models.SuiDevInspectTransactionBlockRequest{
		Sender:  config.wallet,
		TxBytes: txBytes,
	})

	if err != nil {
		log.Printf("error SignAndExecuteTransactionBlock: %v", err)
		return nil, err
	}

	type moveCallResult struct {
		ReturnValues [2]interface{}
	}

	var moveCallReturn []moveCallResult
	err = json.Unmarshal(rsp2.Results, &moveCallReturn)
	if err != nil {
		fmt.Println("json", err.Error())
		return nil, err
	}

	var dlo RecursiveDirList
	t := moveCallReturn[0].ReturnValues
	t1 := t[0].([]interface{})
	t2 := t1[0].([]interface{})
	output := make([]byte, 0, len(t2))
	for i := range t2 {
		bv := byte(int(t2[i].(float64)))
		output = append(output, bv)
	}

	if _, err := bcs.Unmarshal(output, &dlo); err != nil {
		log.Printf("failed to decode: %v", err.Error())
		return nil, err
	}

	res, err := parse_dir_all(&dlo)
	if err != nil {
		fmt.Println("parse_dir_all", err.Error())
		return nil, err
	}

	return &res, nil
}
