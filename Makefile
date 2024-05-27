PROJECT=test1-305123
REGION=us-central1
TOPIC=topic1

all:
	@echo "The Makefile"

init:
	# Create a topic
	-gcloud pubsub topics create $(TOPIC) --project $(PROJECT)
	-gcloud pubsub subscriptions create subscription1 --topic $(TOPIC) --project $(PROJECT)
