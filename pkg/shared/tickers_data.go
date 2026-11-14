package shared

import (
	"sync"
	"time"
)

type TikersDataMu struct {
	Spot       TickersDataSpot
	Perpetual  TickersDataPerpetual
	Futures    []TickersDataFutures
	Options    []TickersDataOptions
	IndexPrice string
	Ts         time.Time
	sync.Mutex
}

type TickersDataSpot struct {
	Symbol    string `json:"symbol"`
	BidPrice  string `json:"bid_price"`
	BidSize   string `json:"bid_size"`
	AskPrice  string `json:"ask_price"`
	AskSize   string `json:"ask_size"`
	LastPrice string `json:"last_price"`
}

type TickersDataPerpetual struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`

	MarkPrice string `json:"mark_price"`

	FundingRate     string `json:"funding_rate"`
	NextFundingTime string `json:"next_funding_time"`
}

type TickersDataFutures struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`

	MarkPrice string `json:"mark_price"`

	BasisRateYear string `json:"basis_rate_year"`
	DeliveryTime  string `json:"delivery_time"`
}

type TickersDataOptions struct {
	Symbol     string `json:"symbol"`
	Series     string `json:"series"`
	ExpiryTime string `json:"expiry_time"`

	UnderlyingPrice string `json:"underlying_price"` // Форвардная цена базового актива (для Bybit — синтетика серии, для MOEX — цена базового фьючерса / расчётный форвард)

	MinMarkIv             [2]string `json:"min_mark_iv"`
	NearestIvToIndexPrice [2]string `json:"nearest_iv_to_index_price"`

	CallIv25Delta [2]string `json:"call_iv_25_delta"`
	PutIv25Delta  [2]string `json:"put_iv_25_delta"`
	SkewIv25Delta string    `json:"skew_iv_25_delta"` //я считаю как CallIv25Delta - PutIv25Delta

	OptionsLines OptionsLines `json:"options_lines"`
}

type OptionsLines struct {
	Strikes []string `json:"strikes"`

	Cmark []string `json:"c_mark"`
	Pmark []string `json:"p_mark"`
	Cask  []string `json:"c_ask"`
	Pask  []string `json:"p_ask"`
	Cbid  []string `json:"c_bid"`
	Pbid  []string `json:"p_bid"`

	Cspread []string `json:"c_spread"`
	Pspread []string `json:"p_spread"`

	Cdelta []string `json:"c_delta"`
	Pdelta []string `json:"p_delta"`
	Cgamma []string `json:"c_gamma"`
	Pgamma []string `json:"p_gamma"`
	Cvega  []string `json:"c_vega"`
	Pvega  []string `json:"p_vega"`
	Ctheta []string `json:"c_theta"`
	Ptheta []string `json:"p_theta"`

	Coi   []string `json:"c_oi"`
	Poi   []string `json:"p_oi"`
	Ctv   []string `json:"c_tv"`
	Ptv   []string `json:"p_tv"`
	Cv24h []string `json:"c_v24h"`
	Pv24h []string `json:"p_v24h"`

	CaskIv []string `json:"c_ask_iv"`
	PaskIv []string `json:"p_ask_iv"`
	CbidIv []string `json:"c_bid_iv"`
	PbidIv []string `json:"p_bid_iv"`
	MarkIv []string `json:"mark_iv"`

	Cgex   []string `json:"c_gex"`
	Pgex   []string `json:"p_gex"`
	Netgex []string `json:"net_gex"`

	CFeeM []string `json:"c_fee_m"`
	PFeeM []string `json:"p_fee_m"`
	CFeeT []string `json:"c_fee_t"`
	PFeeT []string `json:"p_fee_t"`
}
