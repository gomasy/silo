// Copyright (c) 2026 Feng Ruohang
//
// This file is part of Silo Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import xhttp "github.com/minio/minio/internal/http"

// migrationObjectMetadata restores tags removed from the read representation.
// Keep the original revision, including ordered empty states, for the existing
// locked reconciliation at PUT or multipart completion. Moving is not a new
// tagging mutation, and the write must not modify the reader's metadata map.
func migrationObjectMetadata(oi ObjectInfo) map[string]string {
	metadata := cloneMSS(oi.UserDefined)
	delete(metadata, xhttp.AmzObjectTagging)
	if oi.UserTags != "" {
		metadata[xhttp.AmzObjectTagging] = oi.UserTags
	}
	return metadata
}
