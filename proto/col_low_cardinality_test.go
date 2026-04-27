package proto

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestLowCardinalityOf(t *testing.T) {
	v := NewLowCardinality[string](new(ColStr))

	require.NoError(t, v.Prepare())
	require.Equal(t, ColumnType("LowCardinality(String)"), v.Type())
}

func TestLowCardinalityOfStr(t *testing.T) {
	col := (&ColStr{}).LowCardinality()
	v := []string{"foo", "bar", "foo", "foo", "baz"}
	col.AppendArr(v)

	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_low_cardinality_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := (&ColStr{}).LowCardinality()

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		for i, s := range v {
			assert.Equal(t, s, col.Row(i))
		}
		assert.Equal(t, ColumnType("LowCardinality(String)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).LowCardinality()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).LowCardinality()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestArrLowCardinalityStr(t *testing.T) {
	// Array(LowCardinality(String))
	data := [][]string{
		{"foo", "bar", "baz"},
		{"foo"},
		{"bar", "bar"},
		{"foo", "foo"},
		{"bar", "bar", "bar", "bar"},
	}
	col := new(ColStr).LowCardinality().Array()
	rows := len(data)
	for _, v := range data {
		col.Append(v)
	}
	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_low_cardinality_u8_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := new(ColStr).LowCardinality().Array()
		require.NoError(t, dec.DecodeColumn(r, rows))
		requireEqual[[]string](t, col, dec)
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Array(LowCardinality(String))"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := new(ColStr).LowCardinality().Array()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColStr).LowCardinality().Array()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
	t.Run("WriteColumn", checkWriteColumn(col))
}

func TestColLowCardinality_DecodeColumn(t *testing.T) {
	t.Run("Str", func(t *testing.T) {
		const rows = 25
		values := []string{
			"neo",
			"trinity",
			"morpheus",
		}
		col := new(ColStr).LowCardinality()
		for i := 0; i < rows; i++ {
			col.Append(values[i%len(values)])
		}
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, buf.Buf, "col_low_cardinality_i_str_k_8")
		})
		t.Run("Ok", func(t *testing.T) {
			br := bytes.NewReader(buf.Buf)
			r := NewReader(br)
			dec := new(ColStr).LowCardinality()
			require.NoError(t, dec.DecodeColumn(r, rows))
			requireEqual[string](t, col, dec)
			dec.Reset()
			require.Equal(t, 0, dec.Rows())
			require.Equal(t, ColumnTypeLowCardinality.Sub(ColumnTypeString), dec.Type())
		})
		t.Run("EOF", func(t *testing.T) {
			r := NewReader(bytes.NewReader(nil))
			dec := new(ColStr).LowCardinality()
			require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
		})
		t.Run("NoShortRead", func(t *testing.T) {
			dec := new(ColStr).LowCardinality()
			requireNoShortRead(t, buf.Buf, colAware(dec, rows))
		})
	})
	t.Run("Blank", func(t *testing.T) {
		// Blank columns (i.e. row count is zero) are not encoded.
		col := new(ColStr).LowCardinality()
		var buf Buffer
		require.NoError(t, col.Prepare())
		col.EncodeColumn(&buf)

		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), col.Rows()))
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(2)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidMeta", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(0)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidKeyType", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt64+1))
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
}

func TestColLowCardinality_PrepareIsIdempotent(t *testing.T) {
	col := new(ColStr).LowCardinality()
	for i := 0; i < 257; i++ {
		col.Append(fmt.Sprintf("lg-%06d", i+1))
	}

	require.NoError(t, col.Prepare())
	var first Buffer
	col.EncodeColumn(&first)
	firstDec := new(ColStr).LowCardinality()
	require.NoError(t, firstDec.DecodeColumn(first.Reader(), col.Rows()))
	require.Equal(t, col.Values, firstDec.Values)

	require.NoError(t, col.Prepare())
	var second Buffer
	col.EncodeColumn(&second)
	secondDec := new(ColStr).LowCardinality()
	require.NoError(t, secondDec.DecodeColumn(second.Reader(), col.Rows()))
	require.Equal(t, col.Values, secondDec.Values)
}

func TestColLowCardinality_PrepareRebuildsDictionaryOnReuse(t *testing.T) {
	first := makeLCStrings(1, 256)
	second := makeLCStrings(257, 256)

	col := new(ColStr).LowCardinality()
	col.AppendArr(first)
	require.NoError(t, col.Prepare())

	// Simulate retry/batch-object reuse where caller replaces only logical rows.
	col.Values = col.Values[:0]
	col.AppendArr(second)
	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)

	dec := new(ColStr).LowCardinality()
	require.NoError(t, dec.DecodeColumn(buf.Reader(), len(second)))
	require.Equal(t, second, dec.Values)
}

func TestColLowCardinality_PrepareReusesStaleDictionaryState(t *testing.T) {
	first := makeLCStrings(1, 256)
	second := makeLCStrings(257, 256)

	col := new(ColStr).LowCardinality()
	col.AppendArr(first)
	require.NoError(t, col.Prepare())

	require.Len(t, col.kv, len(first))
	require.Equal(t, len(first), col.index.Rows())
	require.Equal(t, 0, col.keys[0])
	require.Equal(t, len(first)-1, col.keys[len(col.keys)-1])

	col.Values = col.Values[:0]
	col.AppendArr(second)
	require.NoError(t, col.Prepare())

	// kv and dictionary were not rebuilt for the new logical batch.
	require.Len(t, col.kv, len(first)+len(second))
	require.Equal(t, len(first)+len(second), col.index.Rows())

	// The second batch got re-numbered from zero while old dictionary entries
	// remained at the front, which creates the observed offset corruption.
	require.Equal(t, 0, col.keys[0])
	require.Equal(t, len(second)-1, col.keys[len(col.keys)-1])

	index, ok := col.index.(*ColStr)
	require.True(t, ok)
	require.Equal(t, first[0], index.Row(0))
	require.Equal(t, first[len(first)-1], index.Row(len(first)-1))
	require.Equal(t, second[0], index.Row(len(first)))
}

func TestColLowCardinality_PrepareSecondPassDowngradesKeyWidthFromStaleLast(t *testing.T) {
	values := makeLCStrings(1, 257)

	col := new(ColStr).LowCardinality()
	col.AppendArr(values)
	require.NoError(t, col.Prepare())
	require.Equal(t, KeyUInt16, col.key)

	require.NoError(t, col.Prepare())

	require.Len(t, col.kv, len(values))
	require.Equal(t, len(values), col.index.Rows())
	require.Equal(t, KeyUInt8, col.key)
	require.Equal(t, 0, col.keys[0])
	require.Equal(t, len(values)-1, col.keys[len(col.keys)-1])
	require.Equal(t, uint8(0), col.keys8[len(col.keys8)-1])
}

func TestColLowCardinality_PrepareKeyBoundary256(t *testing.T) {
	cases := []struct {
		name string
		rows int
		key  CardinalityKey
	}{
		{name: "255", rows: 255, key: KeyUInt8},
		{name: "256", rows: 256, key: KeyUInt8},
		{name: "257", rows: 257, key: KeyUInt16},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := new(ColStr).LowCardinality()
			col.AppendArr(makeLCStrings(1, tc.rows))
			require.NoError(t, col.Prepare())
			require.Equal(t, tc.key, col.key)
		})
	}
}

func TestColLowCardinality_DecodeColumn_ReusedDecoderWithoutReset(t *testing.T) {
	col := new(ColStr).LowCardinality()
	col.AppendArr([]string{"foo", "bar", "baz", "foo"})
	require.NoError(t, col.Prepare())

	var payload Buffer
	col.EncodeColumn(&payload)

	wire := append([]byte(nil), payload.Buf...)
	wire = append(wire, payload.Buf...)

	dec := new(ColStr).LowCardinality()
	r := NewReader(bytes.NewReader(wire))

	require.NoError(t, dec.DecodeColumn(r, col.Rows()))
	require.Equal(t, col.Values, dec.Values)

	require.NoError(t, dec.DecodeColumn(r, col.Rows()))
	require.Equal(t, col.Values, dec.Values)
}

func makeLCStrings(start, count int) []string {
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, fmt.Sprintf("lg-%06d", start+i))
	}
	return values
}
