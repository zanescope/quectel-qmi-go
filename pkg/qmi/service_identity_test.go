package qmi

import "testing"

type clientIDAccessor interface {
	ClientID() uint8
}

func TestAllServiceClientIDAccessors(t *testing.T) {
	const clientID = 0x2a
	tests := []struct {
		name       string
		service    clientIDAccessor
		nilService clientIDAccessor
	}{
		{name: "DMS", service: &DMSService{clientID: clientID}, nilService: (*DMSService)(nil)},
		{name: "IMS", service: &IMSService{clientID: clientID}, nilService: (*IMSService)(nil)},
		{name: "IMSA", service: &IMSAService{clientID: clientID}, nilService: (*IMSAService)(nil)},
		{name: "IMSP", service: &IMSPService{clientID: clientID}, nilService: (*IMSPService)(nil)},
		{name: "NAS", service: &NASService{clientID: clientID}, nilService: (*NASService)(nil)},
		{name: "UIM", service: &UIMService{clientID: clientID}, nilService: (*UIMService)(nil)},
		{name: "VOICE", service: &VOICEService{clientID: clientID}, nilService: (*VOICEService)(nil)},
		{name: "WDA", service: &WDAService{clientID: clientID}, nilService: (*WDAService)(nil)},
		{name: "WDS", service: &WDSService{clientID: clientID}, nilService: (*WDSService)(nil)},
		{name: "WMS", service: &WMSService{clientID: clientID}, nilService: (*WMSService)(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.service.ClientID(); got != clientID {
				t.Fatalf("ClientID()=0x%02x want 0x%02x", got, clientID)
			}
			if got := tt.nilService.ClientID(); got != 0 {
				t.Fatalf("nil ClientID()=0x%02x want 0", got)
			}
		})
	}
}
