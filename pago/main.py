import urllib.request

import psycopg2
import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/payment")
async def payment(order_id, n_seats):
    print(order_id, n_seats)

    conn = psycopg2.connect(
        database="payments", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )

    cur = conn.cursor()

    # 50 is the price of the seat -- this is hardcoded for educational purposes
    total = int(n_seats) * 50
    print(total)
    cur.execute("INSERT INTO public.\"Payment\" (\"OrderId\", \"Total\", \"Status\") VALUES (%s, %s, %s)",
                (order_id, total, "SUCCESS"))

    conn.commit()

    cur.close()
    conn.close()

    # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
    try:
        send_payment(n_seats, order_id)
        return {"Result": "Success"}
    except Exception as e:
        # Colocar Status = FAILED en la tabla de pagos
        conn = psycopg2.connect(
            database="payments", user='postgres', password='postgres', host='127.0.0.1', port='5432'
        )
        cur = conn.cursor()
        cur.execute("UPDATE public.\"Payment\" SET \"Status\" = 'FAILED' WHERE \"OrderId\" = %s", (order_id,))
        conn.commit()
        cur.close()
        conn.close()

        # Avisar a orquestador que hubo un error
        url = "http://localhost:7004/payment-rollback?order_id={}".format(order_id)
        response = urllib.request.urlopen(url)
        _ = response.read()

        raise Exception("Email service is down")


def send_payment(n_seats, order_id):
    print(n_seats, order_id)
    url = "http://localhost:7002/seats?order_id={}&n_seats={}".format(order_id, n_seats)

    response = urllib.request.urlopen(url)
    data = response.read()
    print(data)


@app.get("/payment-rollback")
async def payment_rollback(order_id):
    # Colocar Status = FAILED en la tabla de pagos
    conn = psycopg2.connect(
        database="payments", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )
    cur = conn.cursor()
    cur.execute("UPDATE public.\"Payment\" SET \"Status\" = 'FAILED' WHERE \"OrderId\" = %s", (order_id,))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7001)

# To excecute: python3 -m uvicorn main:app --reload --port 7000
