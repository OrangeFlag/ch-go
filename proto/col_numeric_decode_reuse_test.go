package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColUInt8_DecodeColumn_ReusedDecoderWithoutReset(t *testing.T) {
	src := ColUInt8{1, 2, 3, 4}

	var payload Buffer
	src.EncodeColumn(&payload)

	wire := append([]byte(nil), payload.Buf...)
	wire = append(wire, payload.Buf...)

	var dec ColUInt8
	r := NewReader(bytes.NewReader(wire))

	require.NoError(t, dec.DecodeColumn(r, len(src)))
	require.Equal(t, src, dec)

	require.NoError(t, dec.DecodeColumn(r, len(src)))
	require.Equal(t, src, dec)
}

func TestColUInt16_DecodeColumn_ReusedDecoderWithoutReset(t *testing.T) {
	src := ColUInt16{1, 2, 255, 256, 257}

	var payload Buffer
	src.EncodeColumn(&payload)

	wire := append([]byte(nil), payload.Buf...)
	wire = append(wire, payload.Buf...)

	var dec ColUInt16
	r := NewReader(bytes.NewReader(wire))

	require.NoError(t, dec.DecodeColumn(r, len(src)))
	require.Equal(t, src, dec)

	require.NoError(t, dec.DecodeColumn(r, len(src)))
	require.Equal(t, src, dec)
}
