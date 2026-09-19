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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package lifecycle

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/minio/minio/internal/bucket/object/lock"
)

func TestLifecycleRetiredAccessExtensions(t *testing.T) {
	const accessRule = `<Rule><ID>old-access</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><AccessTransition><Window>10m</Window><PromoteAfterAccesses>100</PromoteAfterAccesses><DemoteAfterAccesses>5</DemoteAfterAccesses><DemoteAfterIdle>24h</DemoteAfterIdle></AccessTransition></Rule>`
	const ordinaryRule = `<Rule><ID>expiry</ID><Status>Enabled</Status><Filter><Prefix>expired/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule>`
	for _, mixed := range []bool{false, true} {
		input := `<LifecycleConfiguration><AccessTierQuota>500GiB</AccessTierQuota>` + accessRule
		if mixed {
			input += ordinaryRule
		}
		input += `</LifecycleConfiguration>`
		lc, err := ParseLifecycleConfig(strings.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		if lc.HasActiveRules("logs/") {
			t.Fatal("retired rule is active")
		}
		if lc.HasActiveRules("expired/") != mixed {
			t.Fatalf("ordinary expiration lost (mixed=%v)", mixed)
		}
		encoded, err := xml.Marshal(lc)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(encoded, []byte("AccessTierQuota")) || bytes.Contains(encoded, []byte("AccessTransition")) {
			t.Fatalf("retired extension re-emitted: %s", encoded)
		}
		if err := lc.Validate(lock.Retention{}); err == nil {
			t.Fatal("actionless rule unexpectedly validates")
		}
		// Operators remove access-only rules before editing the remaining config.
		if mixed {
			lc.Rules = lc.Rules[1:]
			if err := lc.Validate(lock.Retention{}); err != nil {
				t.Fatalf("ordinary ILM edit after removing retired rule: %v", err)
			}
		}
	}
	// A quota on a normal rule is accepted and dropped by the PUT parser too.
	input := `<LifecycleConfiguration><AccessTierQuota>500GiB</AccessTierQuota>` + ordinaryRule + `</LifecycleConfiguration>`
	lc, err := ParseLifecycleConfigWithID(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if err := lc.Validate(lock.Retention{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseLifecycleConfig(strings.NewReader(`<LifecycleConfiguration><Unknown>1</Unknown></LifecycleConfiguration>`)); err == nil {
		t.Fatal("unknown top-level element accepted")
	}
	if _, err := ParseLifecycleConfig(strings.NewReader(`<LifecycleConfiguration><AccessTierQuota><broken></AccessTierQuota></LifecycleConfiguration>`)); err == nil {
		t.Fatal("malformed XML accepted")
	}
}
