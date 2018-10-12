// Package kudo_mocking
// @author Valentino
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

type Kafka struct {
	Hosts  string `json:"hosts"`
	Topics struct {
		PaymentStatus string `json:"payment_status"`
	} `json:"topics"`

}

type Config struct {
	Kafka Kafka `json:"kafka"`
	Listening string `json:"server_listen"`
}

var (
	cnf      = Config{}
	healthy  int32
	producer sarama.SyncProducer
)

type NopProducer struct {
}

// SendMessage produces a given message, and returns only when it either has
// succeeded or failed to produce. It will return the partition and the offset
// of the produced message, or an error if the message failed to produce.
func (x *NopProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (x *NopProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return nil
}

// Close shuts down the producer and waits for any buffered messages to be
// flushed. You must call this function before a producer object passes out of
// scope, as it may otherwise leak memory. You must call this before calling
// Close on the underlying client.
func (x *NopProducer) Close() error {
	return nil
}

func main() {

	producer = &NopProducer{}

	srvMx := mux.NewRouter()

	b, err := ioutil.ReadFile("./config.json")

	if err != nil {
		fmt.Println("error : ", err)
		return
	}

	if err := json.Unmarshal(b, &cnf); err != nil {
		fmt.Println("error : ", err)
		return
	}
	hosts := strings.Split(cnf.Kafka.Hosts, ",")
	producer = newDataCollector(hosts)

	r := srvMx.PathPrefix("/").Subrouter()

	r.HandleFunc("/auth/in/check-token", serveAuth).Methods("GET")
	r.HandleFunc("/service/in/users/customer", customer).Methods("GET", "POST")
	r.HandleFunc("/v3/items/in/price", itemPrice).Methods("GET")
	r.HandleFunc("/service/in/users/agent/{id:[0-9]+}/verify-pin", verifyPin).Methods("GET")
	r.HandleFunc("/payment_router/v1/transaction/{ref:[0-9a-zA-Z_\\-]+}/order-payment", makePayment).Methods("POST")
	r.HandleFunc("/campaign/order/{ref:[0-9a-zA-Z_\\-]+}", checkCampaign).Methods("GET")
	r.HandleFunc("/campaign/cancel-bulk", campaignCancelBulk).Methods("POST")
	r.HandleFunc("/v3/items/in/airtime/product/validate", validateProduct).Methods("POST")

	s := &http.Server{
		Addr:         cnf.Listening, // set our http listener port 		// set our request handler
		Handler:      srvMx,
		ReadTimeout:  time.Duration(10) * time.Second, // set our microservice read timeout (5s)
		WriteTimeout: time.Duration(10) * time.Second, // set our microservice write timeout (5s)
	}

	log.Fatal(s.ListenAndServe())

}

func serveAuth(w http.ResponseWriter, r *http.Request) {

	rsp := `{
    "code": 1000,
    "message": {
        "nis": "20180601000001",
        "cashier_id": "20180601000001",
        "email": "test@test.com",
        "is_agent": true,
        "first_name": "tester",
        "phonenumber": "0821212121",
        "last_name": "auth",
        "active_status": 1
    }
}`

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "close")
	w.Write([]byte(rsp))

	return

}

func customer(w http.ResponseWriter, r *http.Request) {
	rsp := `{
    "code": 1000,
    "message": {
        "id": 10000,
        "phonenumber": "081298298345",
        "name": "test",
        "description": "tester"
    }
}`

	//time.Sleep(20 *time.Second)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))

	return
}

func itemPrice(w http.ResponseWriter, r *http.Request) {

	rsp := `{
    "code": 1000,
    "message": "success",
    "data": [
        {
            "id": 100,
            "name": "Voucher Rp100.000",
            "description": "Voucher Rp100.000",
            "group_id": 403,
            "vendor_id": 83,
            "product_code": "TSV100",
            "price": 100000,
            "b2b_price": 97000,
            "nominal": 100000,
            "discount_price": 0,
            "reseller_price": 97000,
            "commission_price": 2000,
            "is_published": 1,
            "is_maintenance": 0,
            "vendor_name": "Kudo Airtime"
        }
    ]
}`

	//time.Sleep(20 *time.Second)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))
	return

}

func verifyPin(w http.ResponseWriter, r *http.Request) {
	rsp := `{
    "code": 1000,
    "message": {
        "is_verified": true
    }
}`

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))
	return
}

//make payment
func makePayment(w http.ResponseWriter, r *http.Request) {

	ref := r.FormValue("reference")
	pType := r.FormValue("payment_type")
	rsp := `{
  "payment_type": "wallet-` + pType + `",
  "vendor_id": 12,
  "agent_id": 673348,
  "currency": 360,
  "amount": 500000,
  "details": {
    "payment_type": "` + pType + `",
    "vendor_id": 12,
    "agent_id": 673348,
    "currency": 360,
    "amount": 500000,
    "details": {
      "items": [
        {
          "name": "Voucher Telkomsel Rp10.000 (082122428287)",
          "qty": 1,
          "price": 10000,
          "total_price": 10000
        }
      ]
    }
  }
}`

	payload := `{"reference":"` + ref + `","status":"SUCCESS","payment_datetime":"2018-09-26T10:08:02Z","amount":79000,"currency":"IDR","payment_method":"` + pType + `"}`

	go publishToTopic([]byte(payload), cnf.Kafka.Topics.PaymentStatus)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))
	return
}

func publishToTopic(payload []byte, topicName string) {

	// We are not setting a message key, which means that all messages will
	// be distributed randomly over the different partitions.

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder(payload),
	})

	if err != nil {
		fmt.Printf("Failed to store your data:, %s \n", err)
	} else {
		// The tuple (topic, partition, offset) can be used as a unique identifier
		// for a message in a Kafka cluster.
		fmt.Printf("Your data is stored with unique identifier important/%d/%d\n", partition, offset)
	}
}

func newDataCollector(brokerList []string) sarama.SyncProducer {

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	config.Net.TLS.Enable = false

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

func checkCampaign(w http.ResponseWriter, r *http.Request) {

	ref := mux.Vars(r)["ref"]

	fmt.Println(ref)
	rsp := `{
  "data": {
    "reference": "` + ref + `",
    "campaign_id": 174,
    "counter_detail_id": 10, 
    "campaign_type": "0",
    "campaign_code": "GRAB",
    "direct_discount": 25577,
    "shipping_discount": 0
  }
}`

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))

	return
}

// /campaign/cancel-bulk
func campaignCancelBulk(w http.ResponseWriter, r *http.Request) {
	rsp := `{
    "messages": {
        "en": "Promo has been cancel",
        "id": "Promo berhasil dibatalkan"
    }
}`

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))

	return
}

// /v3/items/in/airtime/product/validate
func validateProduct(w http.ResponseWriter, r *http.Request) {

	rsp := `{
    "code": 1000,
    "message": "success",
    "data": {
        "product_code": "TSV100",
        "phone_number": "85850006613",
        "country_calling_code": 62,
        "b2b_price": 97000,
        "price": 100000,
        "name": "Voucher Rp100.000",
        "nominal": 100000,
        "description": "Voucher Rp100.000",
        "item_id": 26178430,
        "currency_code": "IDR",
        "country_code": "ID",
        "product_type": "pulsa"
    }
}`

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(rsp))

	return
}
