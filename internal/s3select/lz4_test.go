// Copyright (c) 2026 Feng Ruohang
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package s3select

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/pierrec/lz4/v4"
)

func TestLZ4Input(t *testing.T) {
	request := []byte(`<SelectObjectContentRequest>
<Expression>SELECT * FROM S3Object</Expression><ExpressionType>SQL</ExpressionType>
<InputSerialization><CompressionType>LZ4</CompressionType><CSV><FileHeaderInfo>NONE</FileHeaderInfo></CSV></InputSerialization>
<OutputSerialization><CSV/></OutputSerialization></SelectObjectContentRequest>`)
	for _, input := range []string{"", "one,1\ntwo,2\n"} {
		t.Run(input, func(t *testing.T) {
			var compressed bytes.Buffer
			writer := lz4.NewWriter(&compressed)
			if _, err := io.WriteString(writer, input); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := evaluateSelectForTest(t, request, compressed.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != input {
				t.Fatalf("result=%q, want %q", got, input)
			}

			reader, err := newProgressReader(io.NopCloser(iotest.OneByteReader(bytes.NewReader(compressed.Bytes()))), lz4Type)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			got, err = io.ReadAll(reader)
			if err != nil || string(got) != input {
				t.Fatalf("fragmented input: result=%q, err=%v", got, err)
			}
			scanned, processed := reader.Stats()
			if scanned != int64(compressed.Len()) || processed != int64(len(input)) {
				t.Fatalf("scanned=%d processed=%d", scanned, processed)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Read(make([]byte, 1)); err == nil {
				t.Fatal("read after Close succeeded")
			}
		})
	}
}
