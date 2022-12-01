# Yet Another Kafka

YaK or Yet Another Kafka is one of the projects that can be chosen as part of the Big
Data Course (UE20CS322) at PES University.<br />
It involves setting up a mini-Kafka on a student's system, complete with a Producer,
Subscriber and a Publish-Subscribe architecture.<br />

## Team Member :

1. [Spoorthi Kalkunte](https://github.com/legbing)
2. [Sundeep A](https://github.com/SundeepA28)
3. [Thrupthi N](https://github.com/thrupthi2002)
4. [Tushar J](https://github.com/TusharJumla30)

## Usage :

* Run the parent.py file using the command -- python3 parent.py  <br />
* in the parent.py you can create topics, you can do this by using the command -- <br />create TopicName NoPartations  <br />
* Now we need to specify the topic name while running the producer-3.py file.  -- <br />python3 producer-3.py TopicName  <br />
  Now you can start writing messages in the  producer, which will get displayed in the consumer that are subscribed to the same topic<br />
* Finally, we can run the consumer.py file by specifing the topic name and </br>
  we can even add an optional flag -- python3 consumer.py TopicName  (OR)  python3 consumer.py TopicName --from_beginning
