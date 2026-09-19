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

package cmd

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/pierrec/lz4/v4"
)

func TestUntarLZ4(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			want := map[string][]byte{}
			if !empty {
				want["small.txt"] = bytes.Repeat([]byte("small object\n"), 10)
				want["large.txt"] = bytes.Repeat([]byte("large object\n"), 32768)
			}
			var compressed bytes.Buffer
			compressor := lz4.NewWriter(&compressed)
			archive := tar.NewWriter(compressor)
			for name, data := range want {
				if err := archive.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data))}); err != nil {
					t.Fatal(err)
				}
				if _, err := archive.Write(data); err != nil {
					t.Fatal(err)
				}
			}
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
			if err := compressor.Close(); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			got := map[string][]byte{}
			err := untar(t.Context(), iotest.HalfReader(bytes.NewReader(compressed.Bytes())), func(r io.Reader, info os.FileInfo, name string) error {
				// Exercise a short first read followed by streaming the remaining content.
				var output bytes.Buffer
				prefix := make([]byte, 7)
				if _, err := io.ReadFull(r, prefix); err != nil {
					return err
				}
				output.Write(prefix)
				if _, err := io.Copy(&output, r); err != nil {
					return err
				}
				if int64(output.Len()) != info.Size() {
					return fmt.Errorf("size mismatch for %s", name)
				}
				mu.Lock()
				got[name] = output.Bytes()
				mu.Unlock()
				return nil
			}, untarOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("objects=%d, want %d", len(got), len(want))
			}
			for name, data := range want {
				if !bytes.Equal(got[name], data) {
					t.Errorf("content mismatch for %s", name)
				}
			}
			t.Run("corrupt-header", func(t *testing.T) {
				damaged := bytes.Clone(compressed.Bytes())
				damaged[6] ^= 0xff
				err := untar(t.Context(), bytes.NewReader(damaged), func(io.Reader, os.FileInfo, string) error {
					t.Error("corrupt header must be rejected before uploading objects")
					return nil
				}, untarOptions{})
				if err == nil {
					t.Fatal("corrupt LZ4 header accepted")
				}
			})
		})
	}
}
