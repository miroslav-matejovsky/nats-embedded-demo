package demo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    Mode
		wantErr bool
	}{
		{name: "both", value: "both", want: ModeBoth},
		{name: "publisher case insensitive", value: "Publisher", want: ModePublisher},
		{name: "subscriber whitespace", value: " subscriber ", want: ModeSubscriber},
		{name: "invalid", value: "worker", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseMode(test.value)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
