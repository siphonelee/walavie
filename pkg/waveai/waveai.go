// Copyright 2025, Command Line Inc.
// SPDX-License-Identifier: Apache-2.0

package waveai

import (
	"context"
	"log"

	"github.com/wavetermdev/waveterm/pkg/telemetry"
	"github.com/wavetermdev/waveterm/pkg/telemetry/telemetrydata"
	"github.com/wavetermdev/waveterm/pkg/wshrpc"
)

const WaveAIPacketstr = "waveai"
const ApiType_Anthropic = "anthropic"
const ApiType_Perplexity = "perplexity"
const APIType_Google = "google"
const APIType_OpenAI = "openai"

type WaveAICmdInfoPacketOutputType struct {
	Model        string `json:"model,omitempty"`
	Created      int64  `json:"created,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
	Message      string `json:"message,omitempty"`
	Error        string `json:"error,omitempty"`
}

func MakeWaveAIPacket() *wshrpc.WaveAIPacketType {
	return &wshrpc.WaveAIPacketType{Type: WaveAIPacketstr}
}

type WaveAICmdInfoChatMessage struct {
	MessageID           int                            `json:"messageid"`
	IsAssistantResponse bool                           `json:"isassistantresponse,omitempty"`
	AssistantResponse   *WaveAICmdInfoPacketOutputType `json:"assistantresponse,omitempty"`
	UserQuery           string                         `json:"userquery,omitempty"`
	UserEngineeredQuery string                         `json:"userengineeredquery,omitempty"`
}

type AIBackend interface {
	StreamCompletion(
		ctx context.Context,
		request wshrpc.WaveAIStreamRequest,
	) chan wshrpc.RespOrErrorUnion[wshrpc.WaveAIPacketType]
}

func IsCloudAIRequest(opts *wshrpc.WaveAIOptsType) bool {
	if opts == nil {
		return true
	}
	return opts.BaseURL == "" && opts.APIToken == ""
}

func makeAIError(err error) wshrpc.RespOrErrorUnion[wshrpc.WaveAIPacketType] {
	return wshrpc.RespOrErrorUnion[wshrpc.WaveAIPacketType]{Error: err}
}

func RunAICommand(ctx context.Context, request wshrpc.WaveAIStreamRequest) chan wshrpc.RespOrErrorUnion[wshrpc.WaveAIPacketType] {
	telemetry.GoUpdateActivityWrap(wshrpc.ActivityUpdate{NumAIReqs: 1}, "RunAICommand")

	endpoint := request.Opts.BaseURL
	if endpoint == "" {
		endpoint = "default"
	}
	var backend AIBackend
	var backendType string
	if request.Opts.APIType == ApiType_Anthropic {
		backend = AnthropicBackend{}
		backendType = ApiType_Anthropic
	} else if request.Opts.APIType == ApiType_Perplexity {
		backend = PerplexityBackend{}
		backendType = ApiType_Perplexity
	} else if request.Opts.APIType == APIType_Google {
		backend = GoogleBackend{}
		backendType = APIType_Google
	} else if IsCloudAIRequest(request.Opts) {
		endpoint = "waveterm cloud"
		request.Opts.APIType = APIType_OpenAI
		request.Opts.Model = "default"
		backend = WaveAICloudBackend{}
		backendType = "wave"
	} else {
		backend = OpenAIBackend{}
		backendType = APIType_OpenAI
	}
	if backend == nil {
		log.Printf("no backend found for %s\n", request.Opts.APIType)
		return nil
	}
	telemetry.GoRecordTEventWrap(&telemetrydata.TEvent{
		Event: "action:runaicmd",
		Props: telemetrydata.TEventProps{
			AiBackendType: backendType,
		},
	})

	// add walrus prompt in context
	request.Prompt = append(request.Prompt, wshrpc.WaveAIPromptMessageType{
		Role: "system",
		Content: `Aside from being a mammal, Walrus also refers to a novel approach to decentralized blob storage, built to operate on top of the Sui blockchain. It’s designed to provide robust, efficient, and scalable storage for decentralized applications (dApps) that require high levels of integrity, availability, and authenticity for their data. Unlike traditional decentralized storage systems that rely on full replication, Walrus optimizes data storage with a new encoding protocol that minimizes replication costs while ensuring data reliability even under byzantine fault conditions. Please tell the difference based on conversation context." \
			If user asks for file operations between walrus and/or local filesystem, please respond with json including following items: operation type, source path, destination path. The json should start and end with markdown token. Some examples: 
			1. User input: "please copy local folder ~/Downloads/test to /temp on walrus", your response: '\u0060\u0060\u0060{"operation": "copy", "src": "~/Downloads/test", dst: "walrus://temp"}\u0060\u0060\u0060'
			2. User input: "I'd like to copy walrus://temp/file.png to ~/Downloads", your response: '\u0060\u0060\u0060{"operation": "copy", "src": "walrus://temp/file.png", dst: "~/Downloads"}\u0060\u0060\u0060'
			`,
		Name: "",
	})

	log.Printf("sending ai chat message to %s endpoint %q using model %s\n", request.Opts.APIType, endpoint, request.Opts.Model)
	return backend.StreamCompletion(ctx, request)
}
