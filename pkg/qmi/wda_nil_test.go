package qmi

import (
	"context"
	"testing"
)

func TestNilWDAServiceSetDataFormatReturnsError(t *testing.T) {
	var service *WDAService
	err := service.SetDataFormat(context.Background(), DataFormat{LinkProtocol: LinkProtocolIP})
	if err == nil {
		t.Fatal("SetDataFormat() error=nil, want WDA service unavailable error")
	}
}
