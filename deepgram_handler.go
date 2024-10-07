package suzu

import (
	"context"
	"encoding/json"
	"errors"
	api "github.com/deepgram/deepgram-go-sdk/pkg/api/listen/v1/websocket/interfaces"
	"github.com/deepgram/deepgram-go-sdk/pkg/client/interfaces"
	"github.com/machinebox/graphql"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	client "github.com/deepgram/deepgram-go-sdk/pkg/client/listen"
	zlog "github.com/rs/zerolog/log"
)

func init() {
	NewServiceHandlerFuncs.register("deg", NewDeepGramHandler)
}

type DeepGramHandler struct {
	Config Config

	ChannelID    string
	ConnectionID string
	SampleRate   uint32
	ChannelCount uint16
	LanguageCode string
	RetryCount   int
	mu           sync.Mutex

	OnResultFunc func(context.Context, io.WriteCloser, string, string, string, any) error
}

func NewDeepGramHandler(config Config, channelID, connectionID string, sampleRate uint32, channelCount uint16, languageCode string, onResultFunc any) serviceHandlerInterface {
	return &DeepGramHandler{
		Config:       config,
		ChannelID:    channelID,
		ConnectionID: connectionID,
		SampleRate:   sampleRate,
		ChannelCount: channelCount,
		LanguageCode: languageCode,
		OnResultFunc: onResultFunc.(func(context.Context, io.WriteCloser, string, string, string, any) error),
	}
}

type DeepGramResult struct {
	ChannelID    *string `json:"channel_id,omitempty"`
	IsFinal      *bool   `json:"is_final,omitempty"`
	DeepGramType *string `json:"deep_gram_type,omitempty"`
	ResultsID    *string `json:"results_id,omitempty"`
	Language     *string `json:"language,omitempty"`
	TranscriptionResult
}

func NewDeepGramResult(channelID, message string, deepGramType string) DeepGramResult {
	return DeepGramResult{
		TranscriptionResult: TranscriptionResult{
			Type:    "deg",
			Message: message,
		},
		ChannelID:    &channelID,
		DeepGramType: &deepGramType,
	}
}

func (dr *DeepGramResult) WithIsFinal(isFinal bool) *DeepGramResult {
	dr.IsFinal = &isFinal
	return dr
}

func (dr *DeepGramResult) WithResultsID(resultsID string) *DeepGramResult {
	dr.ResultsID = &resultsID
	return dr
}

func (dr *DeepGramResult) WithLanguage(language string) *DeepGramResult {
	dr.Language = &language
	return dr
}

func (h *DeepGramHandler) UpdateRetryCount() int {
	defer h.mu.Unlock()
	h.mu.Lock()
	h.RetryCount++
	return h.RetryCount
}

func (h *DeepGramHandler) GetRetryCount() int {
	return h.RetryCount
}

func (h *DeepGramHandler) ResetRetryCount() int {
	defer h.mu.Unlock()
	h.mu.Lock()
	h.RetryCount = 0
	return h.RetryCount
}

type Transcripts struct {
	Transcript string
	Start      float64
}

// MyCallback Implement your own callback
type MyCallback struct {
	sb          *strings.Builder
	h           *DeepGramHandler
	w           *io.PipeWriter
	ctx         context.Context
	transcripts *[]Transcripts
	resultsID   *string
}

func (c MyCallback) Message(mr *api.MessageResponse) error {
	// handle the message
	sentence := strings.TrimSpace(mr.Channel.Alternatives[0].Transcript)

	if len(mr.Channel.Alternatives) == 0 || len(sentence) == 0 {
		return nil
	}

	encoder := json.NewEncoder(c.w)

	// Find transcript and update
	found := false
	for i := range *c.transcripts {
		if (*c.transcripts)[i].Start == mr.Start {
			(*c.transcripts)[i].Transcript = sentence
			found = true
			break
		}
	}
	// If not found, append
	if !found {
		*c.transcripts = append(*c.transcripts, Transcripts{Transcript: sentence, Start: mr.Start})
	}

	var transcriptsStrings []string
	for _, transcript := range *c.transcripts {
		transcriptsStrings = append(transcriptsStrings, transcript.Transcript)
	}
	message := strings.Join(transcriptsStrings, " ")

	result := NewDeepGramResult(c.h.ChannelID, message, mr.Type)
	result.WithIsFinal(false)
	result.WithResultsID(*c.resultsID)
	result.WithLanguage(c.h.LanguageCode)
	if c.h.OnResultFunc != nil {
		if err := c.h.OnResultFunc(c.ctx, c.w, c.h.ChannelID, c.h.ConnectionID, c.h.LanguageCode, result); err != nil {
			if err := encoder.Encode(NewSuzuErrorResponse(err)); err != nil {
				zlog.Error().
					Err(err).
					Str("channel_id", c.h.ChannelID).
					Str("connection_id", c.h.ConnectionID).
					Send()
			}
			c.w.CloseWithError(err)
			return nil
		}
	} else {
		if err := encoder.Encode(result); err != nil {
			c.w.CloseWithError(err)
			return nil
		}
	}

	// Send message to app sync
	SendMessage(message, c.h.ConnectionID, c.h.ChannelID, *c.resultsID, c.h.LanguageCode, c.h.Config)

	return nil
}

func (c MyCallback) Open(ocr *api.OpenResponse) error {
	// handle the open
	return nil
}

func (c MyCallback) Metadata(md *api.MetadataResponse) error {
	// handle the metadata
	return nil
}

func (c MyCallback) SpeechStarted(ssr *api.SpeechStartedResponse) error {
	return nil
}

func (c MyCallback) UtteranceEnd(ur *api.UtteranceEndResponse) error {
	*c.transcripts = []Transcripts{}
	*c.resultsID = strconv.FormatInt(time.Now().UnixMilli(), 10) + c.h.ConnectionID
	return nil
}

func (c MyCallback) Close(ocr *api.CloseResponse) error {
	zlog.Error().
		Err(errors.New(ocr.Type)).
		Str("channel_id", c.h.ChannelID).
		Str("connection_id", c.h.ConnectionID).
		Send()
	c.w.Close()
	return nil
}

func (c MyCallback) Error(er *api.ErrorResponse) error {
	zlog.Error().
		Err(errors.New(er.ErrMsg)).
		Str("channel_id", c.h.ChannelID).
		Str("connection_id", c.h.ConnectionID).
		Send()
	c.w.Close()
	return nil
}

func (c MyCallback) UnhandledEvent(byData []byte) error {
	// handle the unhandled event
	return nil
}

func SendMessage(content string, connectionID string, channel string, resultsID string, language string, config Config) {
	// URL AppSync của bạn
	appsyncURL := config.AppSyncUrl

	// Tạo GraphQL client
	clientGraphQL := graphql.NewClient(appsyncURL)

	// Thêm các header để xác thực
	clientGraphQL.Log = func(s string) { log.Println(s) }

	// Mutation để gửi tin nhắn
	mutation := `
        mutation SendMessage(
            $content: String!
            $connectionId: String!
            $channel: String!
            $resultsId: String!
            $language: String!
        ) {
            sendMessage(
                content: $content
                connectionId: $connectionId
                channel: $channel
                resultsId: $resultsId
                language: $language
        ) {
            id
            content
            connectionId
            channel
            resultsId
            language
            __typename
        }
    }
    `

	req := graphql.NewRequest(mutation)
	req.Var("content", content)
	req.Var("connectionId", connectionID)
	req.Var("channel", channel)
	req.Var("resultsId", resultsID)
	req.Var("language", language)

	req.Header.Set("x-api-key", config.AppSyncApiKey)

	// Gửi yêu cầu đến AppSync
	ctx := context.Background()
	var respData map[string]interface{}
	if err := clientGraphQL.Run(ctx, req, &respData); err != nil {
		log.Fatal(err)
	}

	log.Printf("Message sent: %+v\n", respData)
}

func (h *DeepGramHandler) Handle(ctx context.Context, reader io.Reader) (*io.PipeReader, error) {
	oggReader, oggWriter := io.Pipe()
	go func() {
		defer oggWriter.Close()
		if err := opus2ogg(ctx, reader, oggWriter, h.SampleRate, h.ChannelCount, h.Config); err != nil {
			if !errors.Is(err, io.EOF) {
				zlog.Error().
					Err(err).
					Str("channel_id", h.ChannelID).
					Str("connection_id", h.ConnectionID).
					Send()
			}
			oggWriter.CloseWithError(err)
			return
		}
	}()

	r, w := io.Pipe()
	var transcripts []Transcripts
	resultsID := strconv.FormatInt(time.Now().UnixMilli(), 10) + h.ConnectionID

	// client options
	cOptions := &interfaces.ClientOptions{
		EnableKeepAlive: true,
	}

	// set the Transcription options
	tOptions := &interfaces.LiveTranscriptionOptions{
		Model:       "nova-2",
		Language:    h.LanguageCode,
		Punctuate:   true,
		SmartFormat: true,
		SampleRate:  int(h.SampleRate),
		Encoding:    "ogg-opus",
		Channels:    int(h.ChannelCount),
		// To get UtteranceEnd, the following must be set:
		InterimResults: true,
		UtteranceEndMs: "1000",
		// End of UtteranceEnd settings
	}

	// implement your own callback
	callback := MyCallback{
		sb:          &strings.Builder{},
		w:           w,
		h:           h,
		ctx:         ctx,
		transcripts: &transcripts,
		resultsID:   &resultsID,
	}

	// create a Deepgram client
	dgClient, err := client.NewWebSocket(ctx, h.Config.DeepGramAPIKey, cOptions, tOptions, callback)
	if err != nil {
		zlog.Error().
			Err(err).
			Str("channel_id", h.ChannelID).
			Str("connection_id", h.ConnectionID).
			Send()
		return nil, err
	}

	// connect to the Deepgram service
	bConnected := dgClient.Connect()
	if !bConnected {
		zlog.Error().
			Err(err).
			Str("channel_id", h.ChannelID).
			Str("connection_id", h.ConnectionID).
			Send()
		return nil, errors.New("Client.Connect failed")
	}

	go func() {
		// close the Deepgram client
		defer dgClient.Stop()
		// feed the HTTP stream to the Deepgram client (this is a blocking call)
		if err := dgClient.Stream(oggReader); err != nil {
			zlog.Error().
				Err(err).
				Str("channel_id", h.ChannelID).
				Str("connection_id", h.ConnectionID).
				Send()
			return
		}
	}()

	return r, nil
}
