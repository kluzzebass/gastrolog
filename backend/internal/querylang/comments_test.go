package querylang

import "testing"

func TestStripComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no comments",
			input: "error foo bar",
			want:  "error foo bar",
		},
		{
			name:  "comment at end of line",
			input: "error foo # this is a comment",
			want:  "error foo ",
		},
		{
			name:  "comment on its own line",
			input: "error foo\n# full line comment\n| stats count",
			want:  "error foo\n\n| stats count",
		},
		{
			name:  "multi-line with inline comments",
			input: "last=5m reverse=true remote_host=*\n  | lookup geoip remote_host\n  # | where remote_host_country=\"US\"\n  | stats count by remote_host_country\n  | sort -count",
			want:  "last=5m reverse=true remote_host=*\n  | lookup geoip remote_host\n  \n  | stats count by remote_host_country\n  | sort -count",
		},
		{
			name:  "hash inside double quotes",
			input: `message="error #42" | stats count`,
			want:  `message="error #42" | stats count`,
		},
		{
			name:  "hash inside single quotes",
			input: `message='error #42' | stats count`,
			want:  `message='error #42' | stats count`,
		},
		{
			name:  "hash inside regex",
			input: `message=/error #\d+/ | stats count`,
			want:  `message=/error #\d+/ | stats count`,
		},
		{
			name:  "trailing comment after pipe stage",
			input: "| stats count by src_ip  # per-IP breakdown\n| sort -count",
			want:  "| stats count by src_ip  \n| sort -count",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "only a comment",
			input: "# just a comment",
			want:  "",
		},
		{
			name:  "comment with no trailing newline",
			input: "foo # comment",
			want:  "foo ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StripComments(tt.input)
			if got != tt.want {
				t.Errorf("StripComments(%q)\n  got  %q\n  want %q", tt.input, got, tt.want)
			}
		})
	}
}
