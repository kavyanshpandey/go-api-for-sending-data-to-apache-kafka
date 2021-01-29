//main package
package main

//importing necessary modules
import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log" //This will help with writing logs.
	"net/http"
	"os"
)

//structure for book
type Ledger struct {
	Identifier string `json:"identifier"`
	Group      string `json:"group"`
	Username   string `json:"username"`
	Time       string `json:"time"`
	Status     string `json:"status"`
}

//main function (execution will start from here)
func main() {

	//configuration with kafka using serema.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs", jobsPostHandler).Methods("POST")
	log.Fatal(http.ListenAndServe(":9090", router))

}

func jobsPostHandler(w http.ResponseWriter, r *http.Request) {
	//Retrieve body from http request
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		panic(err)
	}
	//Save data into Book struct
	var _ledger Ledger
	err = json.Unmarshal(b, &_ledger)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	saveJobToKafka(_ledger) //This function will use for saving data into kafka
	//This section will write log in ERROR.LOG file after every successful entry.
	file, e := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if e != nil {
		log.Fatalln("failed")
	}
	log.SetOutput(file)
	log.Println("Record has been successfully inserted.")

	//Convert Book struct into json
	jsonString, err := json.Marshal(_ledger)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	//Set content-type http header
	w.Header().Set("content-type", "application/json")
	//Send back data as response
	w.Write(jsonString)
}
func saveJobToKafka(ledger Ledger) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	fmt.Println("save to kafka")
	jsonString, err := json.Marshal(ledger)
	ledgerString := string(jsonString)
	fmt.Print(ledgerString)

	topic := "my-topic"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(ledgerString),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println("Record has not inserted.")
		panic(err)
	}
	//this print will give a confirmation that our JSON data has been successfully saved in kafka.
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

}
