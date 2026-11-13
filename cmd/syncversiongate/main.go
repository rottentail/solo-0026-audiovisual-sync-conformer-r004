// Command syncversiongate is the version-maintenance admission gate for the
// audiovisual sync conformer.
//
// The service treats take descriptors, sync analyses, and conform receipts as
// durable delivery records. Replacing a detector or a codec is therefore not
// admitted by "the service still starts". This tool replays a committed media
// corpus through the current build and compares the resulting records against
// the committed golden records at three levels:
//
//   - semantic: states, error codes, event times, offsets, applied samples,
//     decoded media content, and the delivery-chain digests. A difference
//     means the candidate changed the meaning of existing results. REJECT.
//   - advisory: confidence and prominence readings. Decisions are unchanged
//     but the readings moved. ADMIT only with an explicit sign-off.
//   - byte: container bytes and digests. Byte drift with equal decoded
//     content is audiovisual-equivalent and never blocks admission.
//
// The gate also re-interprets the committed golden bundles with the current
// detector: a historical bundle whose aligned pair no longer measures within
// +/-2 ms of zero means the candidate changed the meaning of receipts that
// were already delivered, even though every file is still readable.
//
// Modes:
//
//	generate  write the deterministic corpus inputs and corpus.json
//	capture   run the current build and bless its output as the golden records
//	verify    replay the corpus and compare against the golden records
//
// Exit codes for verify: 0 admit, 2 admit with sign-off, 1 reject.
package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/domain"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/httpapi"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/media"
	appservice "github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/service"
	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/storage"
)

const (
	toolVersion         = "syncversiongate/1"
	manifestName        = "corpus.json"
	goldenRecordsName   = "golden/records.json"
	inputsDir           = "inputs"
	bundlesDir          = "golden/bundles"
	residualToleranceMS = 2
)

// ---------------------------------------------------------------------------
// Corpus definition. The generator is deterministic; the rendered inputs are
// committed so the materials stay usable without the generator.
// ---------------------------------------------------------------------------

type takeSpec struct {
	id           string
	name         string
	purpose      string
	calibration  bool
	sampleRate   int
	channels     int
	audioMS      int
	eventSample  int // burst start in sample frames; -1 renders silence
	flash        map[int]int
	videoFrames  int
	minConf      float64
	maxOffsetMS  int
	maxCorrectMS int
}

func corpusSpecs() []takeSpec {
	standard := func() (float64, int, int) { return 0.55, 1000, 250 }
	specs := []takeSpec{}
	minConf, maxOffset, maxCorrect := standard()
	specs = append(specs,
		takeSpec{
			id: "calibration-125", name: "calibration pair, audio 125 ms late",
			purpose:     "production anchor: the same pair the workflow checks use, offset +125 ms",
			calibration: true,
			videoFrames: media.CalibrationFrames,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "negative-drift", name: "audio 80 ms early",
			purpose:    "negative offset: audio is moved later by the conform",
			sampleRate: 48000, channels: 1, audioMS: 1000, eventSample: 20160,
			flash:       map[int]int{12: 220},
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "stereo-44k1", name: "stereo 44.1 kHz, audio 117 ms late",
			purpose:    "two channels and a non-48k rate: 117 ms rounds to 5160 sample frames",
			sampleRate: 44100, channels: 2, audioMS: 1500, eventSample: 34540,
			flash:       map[int]int{16: 220},
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "ramp-flash", name: "two-frame luma ramp, audio 75 ms late",
			purpose:    "flash rises over frames 9-10: exposes which frame a detector calls the event",
			sampleRate: 48000, channels: 1, audioMS: 1000, eventSample: 21600,
			flash:       map[int]int{9: 150, 10: 220},
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "weak-flash", name: "low contrast flash, audio 125 ms late",
			purpose:    "delta 60 luma: confidence 0.875 sits near policy, exposes prominence drift",
			sampleRate: 48000, channels: 1, audioMS: 1000, eventSample: 24000,
			flash:       map[int]int{9: 92},
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "silent-flat", name: "silence and flat luma",
			purpose:    "no events: analysis must fail with sync_detection_failed, conform must be refused",
			sampleRate: 48000, channels: 1, audioMS: 1000, eventSample: -1,
			flash:       nil,
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
		takeSpec{
			id: "beyond-ceiling", name: "audio 417 ms late",
			purpose:    "offset 417 ms exceeds the 250 ms ceiling: conform must fail with correction_limit_exceeded",
			sampleRate: 48000, channels: 1, audioMS: 1000, eventSample: 24000,
			flash:       map[int]int{2: 220},
			videoFrames: 24,
			minConf:     minConf, maxOffsetMS: maxOffset, maxCorrectMS: maxCorrect,
		},
	)
	return specs
}

func (spec takeSpec) buildMedia() ([]byte, []byte, error) {
	if spec.calibration {
		return media.CalibrationPair()
	}
	frames := spec.sampleRate * spec.audioMS / 1000
	samples := make([]int16, frames*spec.channels)
	if spec.eventSample >= 0 {
		for index := spec.eventSample; index < frames; index++ {
			elapsed := float64(index-spec.eventSample) / float64(spec.sampleRate)
			envelope := math.Exp(-elapsed * 7)
			value := math.Sin(2*math.Pi*440*float64(index-spec.eventSample)/float64(spec.sampleRate)) * 12000 * envelope
			sample := int16(math.Round(value))
			for channel := 0; channel < spec.channels; channel++ {
				samples[index*spec.channels+channel] = sample
			}
		}
	}
	audio, err := media.EncodePCM16(spec.sampleRate, spec.channels, samples)
	if err != nil {
		return nil, nil, fmt.Errorf("build audio for %s: %w", spec.id, err)
	}

	width, height := 96, 54
	frameBytes := width*height + 2*(width/2)*(height/2)
	videoFrames := make([][]byte, 0, spec.videoFrames)
	for frameIndex := 0; frameIndex < spec.videoFrames; frameIndex++ {
		frame := make([]byte, frameBytes)
		luma := byte(32)
		if override, ok := spec.flash[frameIndex]; ok {
			luma = byte(override)
		}
		for index := 0; index < width*height; index++ {
			frame[index] = luma
		}
		chromaSize := (width / 2) * (height / 2)
		for index := 0; index < chromaSize; index++ {
			frame[width*height+index] = 128
			frame[width*height+chromaSize+index] = 128
		}
		videoFrames = append(videoFrames, frame)
	}
	info := domain.VideoInfo{
		Width: width, Height: height, FPSNum: 24, FPSDen: 1,
		FrameCount: spec.videoFrames, Chroma: "C420jpeg",
	}
	video, err := media.EncodeY4M(info, videoFrames)
	if err != nil {
		return nil, nil, fmt.Errorf("build video for %s: %w", spec.id, err)
	}
	return audio, video, nil
}

// ---------------------------------------------------------------------------
// Manifest and golden record model.
// ---------------------------------------------------------------------------

type manifestTake struct {
	TakeID          string  `json:"take_id"`
	Name            string  `json:"name"`
	Note            string  `json:"note"`
	Purpose         string  `json:"purpose"`
	Audio           string  `json:"audio"`
	Video           string  `json:"video"`
	MinConfidence   float64 `json:"min_confidence"`
	MaxOffsetMS     int     `json:"max_offset_ms"`
	MaxCorrectionMS int     `json:"max_correction_ms"`
}

type corpusManifest struct {
	SchemaVersion int            `json:"schema_version"`
	Takes         []manifestTake `json:"takes"`
}

type audioSemantic struct {
	SampleRate int     `json:"sample_rate"`
	Channels   int     `json:"channels"`
	BitDepth   int     `json:"bit_depth"`
	FrameCount int     `json:"frame_count"`
	DurationMS int     `json:"duration_ms"`
	PeakDBFS   float64 `json:"peak_dbfs"`
	RMSDBFS    float64 `json:"rms_dbfs"`
}

type videoSemantic struct {
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	FPSNum     int    `json:"fps_num"`
	FPSDen     int    `json:"fps_den"`
	FrameCount int    `json:"frame_count"`
	DurationMS int    `json:"duration_ms"`
	Chroma     string `json:"chroma"`
}

type takeRecordSemantic struct {
	State string        `json:"state"`
	Name  string        `json:"name"`
	Note  string        `json:"note"`
	Audio audioSemantic `json:"audio"`
	Video videoSemantic `json:"video"`
}

type analysisSemantic struct {
	State         string  `json:"state"`
	ErrorCode     string  `json:"error_code"`
	AudioEventMS  *int    `json:"audio_event_ms,omitempty"`
	VideoEventMS  *int    `json:"video_event_ms,omitempty"`
	OffsetMS      *int    `json:"offset_ms,omitempty"`
	DriftPPM      int     `json:"drift_ppm"`
	MinConfidence float64 `json:"min_confidence"`
	MaxOffsetMS   int     `json:"max_offset_ms"`
}

type receiptSemantic struct {
	SchemaVersion    int    `json:"schema_version"`
	ConformID        string `json:"conform_id"`
	AnalysisID       string `json:"analysis_id"`
	TakeID           string `json:"take_id"`
	MeasuredOffsetMS int    `json:"measured_offset_ms"`
	AppliedOffsetMS  int    `json:"applied_offset_ms"`
	AppliedSamples   int    `json:"applied_samples"`
}

type conformSemantic struct {
	Exists             bool             `json:"exists"`
	State              string           `json:"state"`
	ErrorCode          string           `json:"error_code"`
	RequestStatus      int              `json:"request_status"`
	RequestErrorCode   string           `json:"request_error_code"`
	MaxCorrectionMS    int              `json:"max_correction_ms"`
	MeasuredOffsetMS   *int             `json:"measured_offset_ms,omitempty"`
	AppliedOffsetMS    *int             `json:"applied_offset_ms,omitempty"`
	AppliedSamples     *int             `json:"applied_samples,omitempty"`
	BundleAvailable    bool             `json:"bundle_available"`
	AudioSamplesSHA256 string           `json:"audio_samples_sha256,omitempty"`
	VideoFramesSHA256  string           `json:"video_frames_sha256,omitempty"`
	Receipt            *receiptSemantic `json:"receipt,omitempty"`
}

type takeSemanticGroup struct {
	AdmissionStatus int                `json:"admission_status"`
	AnalysisStatus  int                `json:"analysis_status"`
	Take            takeRecordSemantic `json:"take"`
	Analysis        analysisSemantic   `json:"analysis"`
	Conform         conformSemantic    `json:"conform"`
}

type takeAdvisory struct {
	Confidence      float64 `json:"confidence"`
	AudioProminence float64 `json:"audio_prominence"`
	VideoProminence float64 `json:"video_prominence"`
}

type takeByteLevel struct {
	TakeDigest             string `json:"take_digest"`
	AudioDigest            string `json:"audio_digest"`
	AudioBytes             int    `json:"audio_bytes"`
	VideoDigest            string `json:"video_digest"`
	VideoBytes             int    `json:"video_bytes"`
	AudioOutputDigest      string `json:"audio_output_digest,omitempty"`
	VideoOutputDigest      string `json:"video_output_digest,omitempty"`
	BundleDigest           string `json:"bundle_digest,omitempty"`
	BundleBytes            int    `json:"bundle_bytes,omitempty"`
	ReceiptAudioDigest     string `json:"receipt_audio_digest,omitempty"`
	ReceiptVideoDigest     string `json:"receipt_video_digest,omitempty"`
	ReceiptInputTakeDigest string `json:"receipt_input_take_digest,omitempty"`
}

type goldenTake struct {
	TakeID    string            `json:"take_id"`
	Semantic  takeSemanticGroup `json:"semantic"`
	Advisory  takeAdvisory      `json:"advisory"`
	ByteLevel takeByteLevel     `json:"byte_level"`
	Bundle    string            `json:"bundle,omitempty"`
}

type goldenFile struct {
	SchemaVersion int          `json:"schema_version"`
	CapturedBy    string       `json:"captured_by"`
	Takes         []goldenTake `json:"takes"`
}

// ---------------------------------------------------------------------------
// Differences and verdicts.
// ---------------------------------------------------------------------------

const (
	classSemantic = "semantic"
	classAdvisory = "advisory"
	classByte     = "byte"
)

type difference struct {
	TakeID    string `json:"take_id"`
	Class     string `json:"class"`
	Field     string `json:"field"`
	Golden    string `json:"golden"`
	Candidate string `json:"candidate"`
}

type diffList struct {
	items []difference
}

func classRank(class string) int {
	switch class {
	case classSemantic:
		return 3
	case classAdvisory:
		return 2
	case classByte:
		return 1
	}
	return 0
}

func show(value any) string {
	switch typed := value.(type) {
	case *int:
		if typed == nil {
			return "null"
		}
		return strconv.Itoa(*typed)
	case string:
		if typed == "" {
			return `""`
		}
		return typed
	}
	return fmt.Sprint(value)
}

func (d *diffList) add(class, takeID, field string, golden, candidate any) {
	d.items = append(d.items, difference{
		TakeID: takeID, Class: class, Field: field,
		Golden: show(golden), Candidate: show(candidate),
	})
}

func (d *diffList) equalString(class, takeID, field, golden, candidate string) {
	if golden != candidate {
		d.add(class, takeID, field, golden, candidate)
	}
}

func (d *diffList) equalInt(class, takeID, field string, golden, candidate int) {
	if golden != candidate {
		d.add(class, takeID, field, golden, candidate)
	}
}

func (d *diffList) equalBool(class, takeID, field string, golden, candidate bool) {
	if golden != candidate {
		d.add(class, takeID, field, golden, candidate)
	}
}

func (d *diffList) equalFloat(class, takeID, field string, golden, candidate float64) {
	if golden != candidate {
		d.add(class, takeID, field, golden, candidate)
	}
}

func (d *diffList) equalIntPtr(class, takeID, field string, golden, candidate *int) {
	switch {
	case golden == nil && candidate == nil:
	case golden == nil || candidate == nil:
		d.add(class, takeID, field, golden, candidate)
	case *golden != *candidate:
		d.add(class, takeID, field, golden, candidate)
	}
}

// ---------------------------------------------------------------------------
// HTTP harness against the real service, mirroring cmd/syncflowcheck.
// ---------------------------------------------------------------------------

type harness struct {
	root   string
	server *httptest.Server
	client *http.Client
}

func newHarness() (*harness, error) {
	root, err := os.MkdirTemp("", "syncversiongate-*")
	if err != nil {
		return nil, err
	}
	repository, err := storage.New(root)
	if err != nil {
		os.RemoveAll(root)
		return nil, err
	}
	api := httpapi.New(appservice.New(repository))
	return &harness{
		root:   root,
		server: httptest.NewServer(api.Handler()),
		client: &http.Client{Timeout: 20 * time.Second},
	}, nil
}

func (h *harness) close() {
	h.server.Close()
	os.RemoveAll(h.root)
}

func (h *harness) request(method, path string, payload any) (int, []byte, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, h.server.URL+path, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, data, nil
}

func errorCode(body []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return envelope.Error.Code
}

// ---------------------------------------------------------------------------
// Running one corpus take through the real workflows.
// ---------------------------------------------------------------------------

type takeOutcome struct {
	admissionStatus     int
	take                domain.MediaTake
	analysisStatus      int
	analysis            domain.SyncAnalysis
	conformRequestState int
	conformRequestError string
	conform             domain.ConformJob
	conformExists       bool
	bundle              []byte
	bundleStatus        int
	receipt             domain.ConformReceipt
}

func runTake(h *harness, mt manifestTake, audio, video []byte) (*takeOutcome, error) {
	outcome := &takeOutcome{}
	analysisID := "analysis-" + mt.TakeID
	conformID := "conform-" + mt.TakeID

	status, body, err := h.request(http.MethodPost, "/v1/takes", map[string]any{
		"take_id":      mt.TakeID,
		"name":         mt.Name,
		"note":         mt.Note,
		"audio_base64": base64.StdEncoding.EncodeToString(audio),
		"video_base64": base64.StdEncoding.EncodeToString(video),
	})
	if err != nil {
		return nil, err
	}
	outcome.admissionStatus = status
	if status != http.StatusCreated {
		return outcome, fmt.Errorf("admission returned %d: %s", status, body)
	}
	status, body, err = h.request(http.MethodGet, "/v1/takes/"+mt.TakeID, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return outcome, fmt.Errorf("take read returned %d: %s", status, body)
	}
	if err := json.Unmarshal(body, &outcome.take); err != nil {
		return nil, fmt.Errorf("decode take: %w", err)
	}

	status, body, err = h.request(http.MethodPost, "/v1/takes/"+mt.TakeID+"/sync-analyses", map[string]any{
		"analysis_id":    analysisID,
		"min_confidence": mt.MinConfidence,
		"max_offset_ms":  mt.MaxOffsetMS,
	})
	if err != nil {
		return nil, err
	}
	outcome.analysisStatus = status
	if status != http.StatusCreated {
		return outcome, fmt.Errorf("analysis returned %d: %s", status, body)
	}
	status, body, err = h.request(http.MethodGet, "/v1/sync-analyses/"+analysisID, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return outcome, fmt.Errorf("analysis read returned %d: %s", status, body)
	}
	if err := json.Unmarshal(body, &outcome.analysis); err != nil {
		return nil, fmt.Errorf("decode analysis: %w", err)
	}

	status, body, err = h.request(http.MethodPost, "/v1/sync-analyses/"+analysisID+"/conforms", map[string]any{
		"conform_id":        conformID,
		"max_correction_ms": mt.MaxCorrectionMS,
	})
	if err != nil {
		return nil, err
	}
	outcome.conformRequestState = status
	if status != http.StatusCreated && status != http.StatusOK {
		outcome.conformRequestError = errorCode(body)
	}

	status, body, err = h.request(http.MethodGet, "/v1/conforms/"+conformID, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusOK {
		outcome.conformExists = true
		if err := json.Unmarshal(body, &outcome.conform); err != nil {
			return nil, fmt.Errorf("decode conform: %w", err)
		}
	}

	status, body, err = h.request(http.MethodGet, "/v1/conforms/"+conformID+"/bundle", nil)
	if err != nil {
		return nil, err
	}
	outcome.bundleStatus = status
	if status == http.StatusOK {
		outcome.bundle = body
		entries, err := readZip(body)
		if err != nil {
			return outcome, fmt.Errorf("open bundle: %w", err)
		}
		if err := json.Unmarshal(entries["conform-receipt.json"], &outcome.receipt); err != nil {
			return outcome, fmt.Errorf("decode receipt: %w", err)
		}
	}
	return outcome, nil
}

// ---------------------------------------------------------------------------
// Building a comparable record from an outcome.
// ---------------------------------------------------------------------------

func hashAudioSamples(wav []byte) (string, error) {
	clip, err := media.InspectAudio(wav)
	if err != nil {
		return "", err
	}
	raw := make([]byte, len(clip.Samples)*2)
	for index, sample := range clip.Samples {
		binary.LittleEndian.PutUint16(raw[index*2:], uint16(sample))
	}
	return media.DigestBytes(raw), nil
}

func hashVideoFrames(y4m []byte) (string, error) {
	clip, err := media.InspectVideo(y4m)
	if err != nil {
		return "", err
	}
	return media.DigestBytes(bytes.Join(clip.Frames, nil)), nil
}

func buildRecord(mt manifestTake, outcome *takeOutcome) (goldenTake, error) {
	record := goldenTake{TakeID: mt.TakeID}
	record.Semantic.AdmissionStatus = outcome.admissionStatus
	record.Semantic.AnalysisStatus = outcome.analysisStatus

	take := outcome.take
	record.Semantic.Take = takeRecordSemantic{
		State: take.State,
		Name:  take.Name,
		Note:  take.Note,
		Audio: audioSemantic{
			SampleRate: take.Audio.SampleRate,
			Channels:   take.Audio.Channels,
			BitDepth:   take.Audio.BitDepth,
			FrameCount: take.Audio.FrameCount,
			DurationMS: take.Audio.DurationMS,
			PeakDBFS:   take.Audio.PeakDBFS,
			RMSDBFS:    take.Audio.RMSDBFS,
		},
		Video: videoSemantic{
			Width:      take.Video.Width,
			Height:     take.Video.Height,
			FPSNum:     take.Video.FPSNum,
			FPSDen:     take.Video.FPSDen,
			FrameCount: take.Video.FrameCount,
			DurationMS: take.Video.DurationMS,
			Chroma:     take.Video.Chroma,
		},
	}
	record.ByteLevel.TakeDigest = take.Digest
	record.ByteLevel.AudioDigest = take.Audio.Digest
	record.ByteLevel.AudioBytes = take.Audio.ByteCount
	record.ByteLevel.VideoDigest = take.Video.Digest
	record.ByteLevel.VideoBytes = take.Video.ByteCount

	analysis := outcome.analysis
	record.Semantic.Analysis = analysisSemantic{
		State:         analysis.State,
		ErrorCode:     analysis.ErrorCode,
		AudioEventMS:  analysis.AudioEventMS,
		VideoEventMS:  analysis.VideoEventMS,
		OffsetMS:      analysis.OffsetMS,
		DriftPPM:      analysis.DriftPPM,
		MinConfidence: analysis.Policy.MinConfidence,
		MaxOffsetMS:   analysis.Policy.MaxOffsetMS,
	}
	record.Advisory = takeAdvisory{
		Confidence:      analysis.Confidence,
		AudioProminence: analysis.AudioProminence,
		VideoProminence: analysis.VideoProminence,
	}

	conform := conformSemantic{
		Exists:           outcome.conformExists,
		RequestStatus:    outcome.conformRequestState,
		RequestErrorCode: outcome.conformRequestError,
		MaxCorrectionMS:  mt.MaxCorrectionMS,
		BundleAvailable:  outcome.bundleStatus == http.StatusOK,
	}
	if outcome.conformExists {
		conform.State = outcome.conform.State
		conform.ErrorCode = outcome.conform.ErrorCode
		conform.MaxCorrectionMS = outcome.conform.MaxCorrectionMS
		measured := outcome.conform.MeasuredOffsetMS
		applied := outcome.conform.AppliedOffsetMS
		samples := outcome.conform.AppliedSamples
		conform.MeasuredOffsetMS = &measured
		conform.AppliedOffsetMS = &applied
		conform.AppliedSamples = &samples
		record.ByteLevel.AudioOutputDigest = outcome.conform.AudioOutputDigest
		record.ByteLevel.VideoOutputDigest = outcome.conform.VideoOutputDigest
		record.ByteLevel.BundleDigest = outcome.conform.BundleDigest
		record.ByteLevel.BundleBytes = outcome.conform.BundleBytes
	}
	if conform.BundleAvailable {
		entries, err := readZip(outcome.bundle)
		if err != nil {
			return record, err
		}
		audioHash, err := hashAudioSamples(entries["audio-aligned.wav"])
		if err != nil {
			return record, fmt.Errorf("hash aligned audio: %w", err)
		}
		videoHash, err := hashVideoFrames(entries["video-conformed.y4m"])
		if err != nil {
			return record, fmt.Errorf("hash conformed video: %w", err)
		}
		conform.AudioSamplesSHA256 = audioHash
		conform.VideoFramesSHA256 = videoHash
		receipt := outcome.receipt
		conform.Receipt = &receiptSemantic{
			SchemaVersion:    receipt.SchemaVersion,
			ConformID:        receipt.ConformID,
			AnalysisID:       receipt.AnalysisID,
			TakeID:           receipt.TakeID,
			MeasuredOffsetMS: receipt.MeasuredOffsetMS,
			AppliedOffsetMS:  receipt.AppliedOffsetMS,
			AppliedSamples:   receipt.AppliedSamples,
		}
		record.ByteLevel.ReceiptAudioDigest = receipt.AudioDigest
		record.ByteLevel.ReceiptVideoDigest = receipt.VideoDigest
		record.ByteLevel.ReceiptInputTakeDigest = receipt.InputTakeDigest
	}
	record.Semantic.Conform = conform
	return record, nil
}

// ---------------------------------------------------------------------------
// Comparisons.
// ---------------------------------------------------------------------------

func compareRecords(d *diffList, golden, candidate goldenTake) {
	takeID := golden.TakeID
	g, c := golden.Semantic, candidate.Semantic
	d.equalInt(classSemantic, takeID, "admission_status", g.AdmissionStatus, c.AdmissionStatus)
	d.equalInt(classSemantic, takeID, "analysis_status", g.AnalysisStatus, c.AnalysisStatus)

	d.equalString(classSemantic, takeID, "take.state", g.Take.State, c.Take.State)
	d.equalString(classSemantic, takeID, "take.name", g.Take.Name, c.Take.Name)
	d.equalString(classSemantic, takeID, "take.note", g.Take.Note, c.Take.Note)
	d.equalInt(classSemantic, takeID, "take.audio.sample_rate", g.Take.Audio.SampleRate, c.Take.Audio.SampleRate)
	d.equalInt(classSemantic, takeID, "take.audio.channels", g.Take.Audio.Channels, c.Take.Audio.Channels)
	d.equalInt(classSemantic, takeID, "take.audio.bit_depth", g.Take.Audio.BitDepth, c.Take.Audio.BitDepth)
	d.equalInt(classSemantic, takeID, "take.audio.frame_count", g.Take.Audio.FrameCount, c.Take.Audio.FrameCount)
	d.equalInt(classSemantic, takeID, "take.audio.duration_ms", g.Take.Audio.DurationMS, c.Take.Audio.DurationMS)
	d.equalFloat(classSemantic, takeID, "take.audio.peak_dbfs", g.Take.Audio.PeakDBFS, c.Take.Audio.PeakDBFS)
	d.equalFloat(classSemantic, takeID, "take.audio.rms_dbfs", g.Take.Audio.RMSDBFS, c.Take.Audio.RMSDBFS)
	d.equalInt(classSemantic, takeID, "take.video.width", g.Take.Video.Width, c.Take.Video.Width)
	d.equalInt(classSemantic, takeID, "take.video.height", g.Take.Video.Height, c.Take.Video.Height)
	d.equalInt(classSemantic, takeID, "take.video.fps_num", g.Take.Video.FPSNum, c.Take.Video.FPSNum)
	d.equalInt(classSemantic, takeID, "take.video.fps_den", g.Take.Video.FPSDen, c.Take.Video.FPSDen)
	d.equalInt(classSemantic, takeID, "take.video.frame_count", g.Take.Video.FrameCount, c.Take.Video.FrameCount)
	d.equalInt(classSemantic, takeID, "take.video.duration_ms", g.Take.Video.DurationMS, c.Take.Video.DurationMS)
	d.equalString(classSemantic, takeID, "take.video.chroma", g.Take.Video.Chroma, c.Take.Video.Chroma)

	d.equalString(classSemantic, takeID, "analysis.state", g.Analysis.State, c.Analysis.State)
	d.equalString(classSemantic, takeID, "analysis.error_code", g.Analysis.ErrorCode, c.Analysis.ErrorCode)
	d.equalIntPtr(classSemantic, takeID, "analysis.audio_event_ms", g.Analysis.AudioEventMS, c.Analysis.AudioEventMS)
	d.equalIntPtr(classSemantic, takeID, "analysis.video_event_ms", g.Analysis.VideoEventMS, c.Analysis.VideoEventMS)
	d.equalIntPtr(classSemantic, takeID, "analysis.offset_ms", g.Analysis.OffsetMS, c.Analysis.OffsetMS)
	d.equalInt(classSemantic, takeID, "analysis.drift_ppm", g.Analysis.DriftPPM, c.Analysis.DriftPPM)
	d.equalFloat(classSemantic, takeID, "analysis.min_confidence", g.Analysis.MinConfidence, c.Analysis.MinConfidence)
	d.equalInt(classSemantic, takeID, "analysis.max_offset_ms", g.Analysis.MaxOffsetMS, c.Analysis.MaxOffsetMS)

	d.equalBool(classSemantic, takeID, "conform.exists", g.Conform.Exists, c.Conform.Exists)
	d.equalString(classSemantic, takeID, "conform.state", g.Conform.State, c.Conform.State)
	d.equalString(classSemantic, takeID, "conform.error_code", g.Conform.ErrorCode, c.Conform.ErrorCode)
	d.equalInt(classSemantic, takeID, "conform.request_status", g.Conform.RequestStatus, c.Conform.RequestStatus)
	d.equalString(classSemantic, takeID, "conform.request_error_code", g.Conform.RequestErrorCode, c.Conform.RequestErrorCode)
	d.equalInt(classSemantic, takeID, "conform.max_correction_ms", g.Conform.MaxCorrectionMS, c.Conform.MaxCorrectionMS)
	d.equalIntPtr(classSemantic, takeID, "conform.measured_offset_ms", g.Conform.MeasuredOffsetMS, c.Conform.MeasuredOffsetMS)
	d.equalIntPtr(classSemantic, takeID, "conform.applied_offset_ms", g.Conform.AppliedOffsetMS, c.Conform.AppliedOffsetMS)
	d.equalIntPtr(classSemantic, takeID, "conform.applied_samples", g.Conform.AppliedSamples, c.Conform.AppliedSamples)
	d.equalBool(classSemantic, takeID, "conform.bundle_available", g.Conform.BundleAvailable, c.Conform.BundleAvailable)
	d.equalString(classSemantic, takeID, "conform.audio_samples_sha256", g.Conform.AudioSamplesSHA256, c.Conform.AudioSamplesSHA256)
	d.equalString(classSemantic, takeID, "conform.video_frames_sha256", g.Conform.VideoFramesSHA256, c.Conform.VideoFramesSHA256)
	compareReceipt(d, takeID, g.Conform.Receipt, c.Conform.Receipt)

	d.equalFloat(classAdvisory, takeID, "analysis.confidence", golden.Advisory.Confidence, candidate.Advisory.Confidence)
	d.equalFloat(classAdvisory, takeID, "analysis.audio_prominence", golden.Advisory.AudioProminence, candidate.Advisory.AudioProminence)
	d.equalFloat(classAdvisory, takeID, "analysis.video_prominence", golden.Advisory.VideoProminence, candidate.Advisory.VideoProminence)

	d.equalString(classByte, takeID, "take.digest", golden.ByteLevel.TakeDigest, candidate.ByteLevel.TakeDigest)
	d.equalString(classByte, takeID, "take.audio.digest", golden.ByteLevel.AudioDigest, candidate.ByteLevel.AudioDigest)
	d.equalInt(classByte, takeID, "take.audio.byte_count", golden.ByteLevel.AudioBytes, candidate.ByteLevel.AudioBytes)
	d.equalString(classByte, takeID, "take.video.digest", golden.ByteLevel.VideoDigest, candidate.ByteLevel.VideoDigest)
	d.equalInt(classByte, takeID, "take.video.byte_count", golden.ByteLevel.VideoBytes, candidate.ByteLevel.VideoBytes)
	d.equalString(classByte, takeID, "conform.audio_output_digest", golden.ByteLevel.AudioOutputDigest, candidate.ByteLevel.AudioOutputDigest)
	d.equalString(classByte, takeID, "conform.video_output_digest", golden.ByteLevel.VideoOutputDigest, candidate.ByteLevel.VideoOutputDigest)
	d.equalString(classByte, takeID, "conform.bundle_digest", golden.ByteLevel.BundleDigest, candidate.ByteLevel.BundleDigest)
	d.equalInt(classByte, takeID, "conform.bundle_bytes", golden.ByteLevel.BundleBytes, candidate.ByteLevel.BundleBytes)
	d.equalString(classByte, takeID, "receipt.audio_digest", golden.ByteLevel.ReceiptAudioDigest, candidate.ByteLevel.ReceiptAudioDigest)
	d.equalString(classByte, takeID, "receipt.video_digest", golden.ByteLevel.ReceiptVideoDigest, candidate.ByteLevel.ReceiptVideoDigest)
	d.equalString(classByte, takeID, "receipt.input_take_digest", golden.ByteLevel.ReceiptInputTakeDigest, candidate.ByteLevel.ReceiptInputTakeDigest)
}

func compareReceipt(d *diffList, takeID string, golden, candidate *receiptSemantic) {
	if golden == nil && candidate == nil {
		return
	}
	if golden == nil || candidate == nil {
		d.add(classSemantic, takeID, "conform.receipt", golden != nil, candidate != nil)
		return
	}
	d.equalInt(classSemantic, takeID, "receipt.schema_version", golden.SchemaVersion, candidate.SchemaVersion)
	d.equalString(classSemantic, takeID, "receipt.conform_id", golden.ConformID, candidate.ConformID)
	d.equalString(classSemantic, takeID, "receipt.analysis_id", golden.AnalysisID, candidate.AnalysisID)
	d.equalString(classSemantic, takeID, "receipt.take_id", golden.TakeID, candidate.TakeID)
	d.equalInt(classSemantic, takeID, "receipt.measured_offset_ms", golden.MeasuredOffsetMS, candidate.MeasuredOffsetMS)
	d.equalInt(classSemantic, takeID, "receipt.applied_offset_ms", golden.AppliedOffsetMS, candidate.AppliedOffsetMS)
	d.equalInt(classSemantic, takeID, "receipt.applied_samples", golden.AppliedSamples, candidate.AppliedSamples)
}

// checkDeliveryChain verifies that one delivered bundle agrees with its
// records: record digests, receipt digests, and the bundle bytes must form one
// consistent chain, and the aligned pair must still measure as conformed.
// The label identifies which side is being checked ("candidate" or
// "historical bundle").
func checkDeliveryChain(d *diffList, takeID, label string, record goldenTake, bundle []byte) {
	if !record.Semantic.Conform.BundleAvailable {
		return
	}
	prefix := label + " chain"
	if got := media.DigestBytes(bundle); got != record.ByteLevel.BundleDigest {
		d.add(classSemantic, takeID, prefix+": bundle digest matches record", record.ByteLevel.BundleDigest, got)
	}
	if len(bundle) != record.ByteLevel.BundleBytes {
		d.add(classSemantic, takeID, prefix+": bundle byte count matches record", record.ByteLevel.BundleBytes, len(bundle))
	}
	entries, err := readZip(bundle)
	if err != nil {
		d.add(classSemantic, takeID, prefix+": bundle opens", "readable zip", err.Error())
		return
	}
	audio, audioOK := entries["audio-aligned.wav"]
	video, videoOK := entries["video-conformed.y4m"]
	receiptData, receiptOK := entries["conform-receipt.json"]
	if !audioOK || !videoOK || !receiptOK {
		d.add(classSemantic, takeID, prefix+": bundle entries", "audio-aligned.wav, video-conformed.y4m, conform-receipt.json", entryNames(entries))
		return
	}
	if got := media.DigestBytes(audio); got != record.ByteLevel.ReceiptAudioDigest {
		d.add(classSemantic, takeID, prefix+": receipt audio digest matches bundle", record.ByteLevel.ReceiptAudioDigest, got)
	}
	if got := media.DigestBytes(video); got != record.ByteLevel.ReceiptVideoDigest {
		d.add(classSemantic, takeID, prefix+": receipt video digest matches bundle", record.ByteLevel.ReceiptVideoDigest, got)
	}
	if got := media.DigestBytes(audio); got != record.ByteLevel.AudioOutputDigest {
		d.add(classSemantic, takeID, prefix+": record audio digest matches bundle", record.ByteLevel.AudioOutputDigest, got)
	}
	if got := media.DigestBytes(video); got != record.ByteLevel.VideoOutputDigest {
		d.add(classSemantic, takeID, prefix+": record video digest matches bundle", record.ByteLevel.VideoOutputDigest, got)
	}
	if record.ByteLevel.ReceiptInputTakeDigest != record.ByteLevel.TakeDigest {
		d.add(classSemantic, takeID, prefix+": receipt names the admitted take",
			record.ByteLevel.TakeDigest, record.ByteLevel.ReceiptInputTakeDigest)
	}
	var receipt domain.ConformReceipt
	if err := json.Unmarshal(receiptData, &receipt); err != nil {
		d.add(classSemantic, takeID, prefix+": receipt parses", "valid receipt json", err.Error())
		return
	}
	measurement, err := media.AnalyzeSync(audio, video)
	if err != nil {
		d.add(classSemantic, takeID, prefix+": aligned pair stays analyzable", "detectable events", err.Error())
		return
	}
	if measurement.OffsetMS < -residualToleranceMS || measurement.OffsetMS > residualToleranceMS {
		d.add(classSemantic, takeID, prefix+": aligned pair still measures conformed",
			fmt.Sprintf("residual offset within +/-%d ms", residualToleranceMS),
			fmt.Sprintf("residual offset %d ms", measurement.OffsetMS))
	}
}

func entryNames(entries map[string][]byte) string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func readZip(data []byte) (map[string][]byte, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open bundle: %w", err)
	}
	entries := make(map[string][]byte)
	for _, entry := range reader.File {
		if strings.Contains(entry.Name, "..") {
			return nil, fmt.Errorf("bundle entry path is unsafe")
		}
		file, err := entry.Open()
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return nil, err
		}
		entries[entry.Name] = content
	}
	return entries, nil
}

// ---------------------------------------------------------------------------
// Modes.
// ---------------------------------------------------------------------------

func loadManifest(corpusDir string) (corpusManifest, error) {
	var manifest corpusManifest
	data, err := os.ReadFile(filepath.Join(corpusDir, manifestName))
	if err != nil {
		return manifest, fmt.Errorf("read %s: %w", manifestName, err)
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, fmt.Errorf("decode %s: %w", manifestName, err)
	}
	if manifest.SchemaVersion != 1 {
		return manifest, fmt.Errorf("unsupported corpus schema version %d", manifest.SchemaVersion)
	}
	return manifest, nil
}

func loadGolden(corpusDir string) (goldenFile, error) {
	var golden goldenFile
	data, err := os.ReadFile(filepath.Join(corpusDir, goldenRecordsName))
	if err != nil {
		return golden, fmt.Errorf("read %s: %w", goldenRecordsName, err)
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		return golden, fmt.Errorf("decode %s: %w", goldenRecordsName, err)
	}
	if golden.SchemaVersion != 1 {
		return golden, fmt.Errorf("unsupported golden schema version %d", golden.SchemaVersion)
	}
	return golden, nil
}

func readInputs(corpusDir string, mt manifestTake) ([]byte, []byte, error) {
	audio, err := os.ReadFile(filepath.Join(corpusDir, mt.Audio))
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", mt.Audio, err)
	}
	video, err := os.ReadFile(filepath.Join(corpusDir, mt.Video))
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", mt.Video, err)
	}
	return audio, video, nil
}

func runGenerate(corpusDir string, force bool) error {
	manifestPath := filepath.Join(corpusDir, manifestName)
	if _, err := os.Stat(manifestPath); err == nil && !force {
		return fmt.Errorf("%s already exists; pass -force to regenerate the corpus inputs", manifestPath)
	}
	manifest := corpusManifest{SchemaVersion: 1}
	for _, spec := range corpusSpecs() {
		audio, video, err := spec.buildMedia()
		if err != nil {
			return err
		}
		if _, err := media.InspectAudio(audio); err != nil {
			return fmt.Errorf("generated audio for %s does not probe: %w", spec.id, err)
		}
		if _, err := media.InspectVideo(video); err != nil {
			return fmt.Errorf("generated video for %s does not probe: %w", spec.id, err)
		}
		audioRel := filepath.Join(inputsDir, spec.id+".wav")
		videoRel := filepath.Join(inputsDir, spec.id+".y4m")
		if err := os.MkdirAll(filepath.Join(corpusDir, inputsDir), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(corpusDir, audioRel), audio, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(corpusDir, videoRel), video, 0o644); err != nil {
			return err
		}
		manifest.Takes = append(manifest.Takes, manifestTake{
			TakeID:          spec.id,
			Name:            spec.name,
			Note:            "syncversiongate corpus",
			Purpose:         spec.purpose,
			Audio:           audioRel,
			Video:           videoRel,
			MinConfidence:   spec.minConf,
			MaxOffsetMS:     spec.maxOffsetMS,
			MaxCorrectionMS: spec.maxCorrectMS,
		})
		fmt.Printf("generated %s (%d audio bytes, %d video bytes)\n", spec.id, len(audio), len(video))
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s with %d takes\n", manifestPath, len(manifest.Takes))
	return nil
}

func runCapture(corpusDir string, force bool) error {
	manifest, err := loadManifest(corpusDir)
	if err != nil {
		return err
	}
	goldenPath := filepath.Join(corpusDir, goldenRecordsName)
	if _, err := os.Stat(goldenPath); err == nil && !force {
		return fmt.Errorf("%s already exists; pass -force to re-bless the golden records", goldenPath)
	}
	server, err := newHarness()
	if err != nil {
		return err
	}
	defer server.close()

	golden := goldenFile{SchemaVersion: 1, CapturedBy: toolVersion}
	for _, mt := range manifest.Takes {
		audio, video, err := readInputs(corpusDir, mt)
		if err != nil {
			return err
		}
		outcome, err := runTake(server, mt, audio, video)
		if err != nil {
			return fmt.Errorf("take %s: %w", mt.TakeID, err)
		}
		record, err := buildRecord(mt, outcome)
		if err != nil {
			return fmt.Errorf("take %s: %w", mt.TakeID, err)
		}
		selfCheck := &diffList{}
		checkDeliveryChain(selfCheck, mt.TakeID, "capture", record, outcome.bundle)
		if len(selfCheck.items) > 0 {
			for _, item := range selfCheck.items {
				fmt.Fprintf(os.Stderr, "capture self-check %s: %s: %s vs %s\n",
					item.TakeID, item.Field, item.Golden, item.Candidate)
			}
			return fmt.Errorf("take %s: the current build fails its own delivery-chain checks; refusing to bless it", mt.TakeID)
		}
		if record.Semantic.Conform.BundleAvailable {
			bundleRel := filepath.Join(bundlesDir, "conform-"+mt.TakeID+".zip")
			if err := os.MkdirAll(filepath.Join(corpusDir, bundlesDir), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(corpusDir, bundleRel), outcome.bundle, 0o644); err != nil {
				return err
			}
			record.Bundle = bundleRel
		}
		golden.Takes = append(golden.Takes, record)
		fmt.Printf("captured %s: analysis=%s conform=%s bundle=%t\n",
			mt.TakeID, record.Semantic.Analysis.State, record.Semantic.Conform.State,
			record.Semantic.Conform.BundleAvailable)
	}
	data, err := json.MarshalIndent(golden, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(goldenPath, append(data, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s with %d takes\n", goldenPath, len(golden.Takes))
	return nil
}

type takeVerdict struct {
	TakeID  string `json:"take_id"`
	Verdict string `json:"verdict"`
}

type gateReport struct {
	Tool        string        `json:"tool"`
	Corpus      string        `json:"corpus"`
	Verdict     string        `json:"verdict"`
	ExitCode    int           `json:"exit_code"`
	Takes       []takeVerdict `json:"takes"`
	Differences []difference  `json:"differences"`
}

func runVerify(corpusDir, reportPath string) int {
	manifest, err := loadManifest(corpusDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	golden, err := loadGolden(corpusDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	goldenByID := make(map[string]goldenTake)
	for _, record := range golden.Takes {
		goldenByID[record.TakeID] = record
	}

	server, err := newHarness()
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	defer server.close()

	diffs := &diffList{}
	for _, mt := range manifest.Takes {
		goldenRecord, ok := goldenByID[mt.TakeID]
		if !ok {
			diffs.add(classSemantic, mt.TakeID, "golden record", "present", "missing")
			continue
		}
		audio, video, err := readInputs(corpusDir, mt)
		if err != nil {
			diffs.add(classSemantic, mt.TakeID, "corpus inputs", "readable", err.Error())
			continue
		}
		if got := media.DigestBytes(audio); got != goldenRecord.ByteLevel.AudioDigest {
			diffs.add(classSemantic, mt.TakeID, "corpus inputs match golden records (audio)",
				goldenRecord.ByteLevel.AudioDigest, got)
		}
		if got := media.DigestBytes(video); got != goldenRecord.ByteLevel.VideoDigest {
			diffs.add(classSemantic, mt.TakeID, "corpus inputs match golden records (video)",
				goldenRecord.ByteLevel.VideoDigest, got)
		}
		outcome, err := runTake(server, mt, audio, video)
		if err != nil {
			diffs.add(classSemantic, mt.TakeID, "workflow run", "flows complete", err.Error())
			continue
		}
		candidate, err := buildRecord(mt, outcome)
		if err != nil {
			diffs.add(classSemantic, mt.TakeID, "record assembly", "records assembled", err.Error())
			continue
		}
		compareRecords(diffs, goldenRecord, candidate)
		checkDeliveryChain(diffs, mt.TakeID, "candidate", candidate, outcome.bundle)

		if goldenRecord.Bundle == "" {
			continue
		}
		goldenBundle, err := os.ReadFile(filepath.Join(corpusDir, goldenRecord.Bundle))
		if err != nil {
			diffs.add(classSemantic, mt.TakeID, "historical bundle", "readable", err.Error())
			continue
		}
		checkDeliveryChain(diffs, mt.TakeID, "historical bundle", goldenRecord, goldenBundle)
		if outcome.bundleStatus == http.StatusOK {
			goldenEntries, err := readZip(goldenBundle)
			if err == nil {
				candidateEntries, err := readZip(outcome.bundle)
				if err == nil {
					if got := entryNames(candidateEntries); got != entryNames(goldenEntries) {
						diffs.add(classSemantic, mt.TakeID, "bundle entry set",
							entryNames(goldenEntries), got)
					}
				}
			}
		}
	}
	for _, record := range golden.Takes {
		found := false
		for _, mt := range manifest.Takes {
			if mt.TakeID == record.TakeID {
				found = true
				break
			}
		}
		if !found {
			diffs.add(classSemantic, record.TakeID, "corpus manifest", "listed", "missing from manifest")
		}
	}

	rankByTake := make(map[string]int)
	for _, item := range diffs.items {
		if classRank(item.Class) > rankByTake[item.TakeID] {
			rankByTake[item.TakeID] = classRank(item.Class)
		}
	}
	verdicts := []takeVerdict{}
	overall := 0
	for _, mt := range manifest.Takes {
		rank := rankByTake[mt.TakeID]
		label := "equivalent"
		switch rank {
		case 3:
			label = "semantic-drift"
		case 2:
			label = "advisory-drift"
		case 1:
			label = "byte-drift (audiovisual-equivalent)"
		}
		verdicts = append(verdicts, takeVerdict{TakeID: mt.TakeID, Verdict: label})
		if rank > overall {
			overall = rank
		}
	}

	fmt.Printf("syncversiongate verify - corpus %s (%d takes)\n", corpusDir, len(manifest.Takes))
	for _, verdict := range verdicts {
		fmt.Printf("  [%-15s] %s\n", verdict.TakeID, verdict.Verdict)
	}
	if len(diffs.items) > 0 {
		fmt.Println("differences:")
		for _, item := range diffs.items {
			if item.Class == classByte {
				continue
			}
			fmt.Printf("  [%s] %s %s: %s -> %s\n", item.Class, item.TakeID, item.Field, item.Golden, item.Candidate)
		}
		byteCount := 0
		for _, item := range diffs.items {
			if item.Class == classByte {
				byteCount++
			}
		}
		if byteCount > 0 {
			fmt.Printf("  [byte] %d byte-level difference(s) with equal decoded content (audiovisual-equivalent)\n", byteCount)
			for _, item := range diffs.items {
				if item.Class == classByte {
					fmt.Printf("         %s %s\n", item.TakeID, item.Field)
				}
			}
		}
	}

	verdict := "ADMIT"
	exitCode := 0
	switch overall {
	case 3:
		verdict = "REJECT"
		exitCode = 1
	case 2:
		verdict = "ADMIT WITH SIGN-OFF"
		exitCode = 2
	}
	fmt.Printf("verdict: %s\n", verdict)

	if reportPath != "" {
		report := gateReport{
			Tool:        toolVersion,
			Corpus:      corpusDir,
			Verdict:     verdict,
			ExitCode:    exitCode,
			Takes:       verdicts,
			Differences: diffs.items,
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			if err := os.WriteFile(reportPath, append(data, '\n'), 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "verify: write report: %v\n", err)
			}
		}
	}
	return exitCode
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	corpus := flag.String("corpus", "maintenance/corpus", "corpus directory")
	force := flag.Bool("force", false, "overwrite existing generated artifacts")
	report := flag.String("report", "", "write a machine-readable JSON report (verify mode)")
	flag.Parse()
	if flag.NArg() != 1 {
		fatalf("usage: syncversiongate [-corpus dir] [-force] [-report file] generate|capture|verify")
	}
	mode := flag.Arg(0)

	var err error
	switch mode {
	case "generate":
		err = runGenerate(*corpus, *force)
	case "capture":
		err = runCapture(*corpus, *force)
	case "verify":
		os.Exit(runVerify(*corpus, *report))
	default:
		fatalf("unknown mode %q: want generate, capture, or verify", mode)
	}
	if err != nil {
		fatalf("%s failed: %v", mode, err)
	}
}
