import urllib.request

import pika


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.100.253.141'))
    channel = connection.channel()

    channel.queue_declare(queue='payment-rollback')


    def callback(ch, method, properties, body):
        print(" [x] Mensaje recibido !! %r" % body)
        url = "http://localhost:7000/order-rollback?order_id={}".format(body.decode("utf-8"))
        response = urllib.request.urlopen(url)
        _ = response.read()


    channel.basic_consume(queue='payment-rollback', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
