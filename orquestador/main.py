import uvicorn
import pika
from fastapi import FastAPI

app = FastAPI()


@app.get("/notification-rollback")
async def notification_rollback(order_id):
    print(order_id)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.100.253.141'))
    channel = connection.channel()
    channel.queue_declare(queue='notification-rollback')
    channel.basic_publish(exchange='', routing_key='notification-rollback', body=order_id)
    connection.close()


@app.get("/seats-rollback")
async def seats_rollback(order_id):
    print(order_id)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.100.253.141'))
    channel = connection.channel()
    channel.queue_declare(queue='seats-rollback')
    channel.basic_publish(exchange='', routing_key='seats-rollback', body=order_id)
    connection.close()


@app.get("/payment-rollback")
async def payment_rollback(order_id):
    print(order_id)

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.100.253.141'))
    channel = connection.channel()
    channel.queue_declare(queue='payment-rollback')
    channel.basic_publish(exchange='', routing_key='payment-rollback', body=order_id)
    connection.close()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7004)