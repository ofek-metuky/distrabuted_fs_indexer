commands:

docker run -p 27017:27017 -d mongo (add volume for persistent storage)

docker run -p 5672:5672 -p 15672:15672 -p -d --hostname my-rabbit rabbitmq:3-management