import urllib.request

import psycopg2
import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/order")
async def order(client_id, n_seats):
    print(client_id, n_seats)

    conn = psycopg2.connect(
        database="orders", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )

    cur = conn.cursor()

    cur.execute("INSERT INTO public.\"Order\" (\"NumberOfSeats\", \"ClientID\", \"Status\") VALUES (%s, %s, %s)",
                (n_seats, client_id, "SUCCESS"))

    conn.commit()

    cur.execute("SELECT MAX(\"ID\") FROM public.\"Order\";")
    order_id = cur.fetchone()

    cur.close()
    conn.close()

    # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
    try:
        send_order(client_id, n_seats, order_id)
        return {"Result": "Success"}
    except Exception as e:
        # Colocar Status = FAILED en la tabla de ordenes
        conn = psycopg2.connect(
            database="orders", user='postgres', password='postgres', host='127.0.0.1', port='5432'
        )
        cur = conn.cursor()
        cur.execute("UPDATE public.\"Order\" SET \"Status\" = 'FAILED' WHERE \"ID\" = %s", (order_id,))
        conn.commit()
        cur.close()
        conn.close()

        raise Exception("Email service is down")


def send_order(client_id, n_seats, order_id):
    print(client_id, n_seats, order_id[0])
    url = "http://localhost:7001/payment?order_id={}&n_seats={}".format(order_id[0], n_seats)

    response = urllib.request.urlopen(url)
    data = response.read()
    print(data)


@app.get("/order-rollback")
async def order_rollback(order_id):
    # Colocar Status = FAILED en la tabla de ordenes
    conn = psycopg2.connect(
        database="orders", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )
    cur = conn.cursor()
    cur.execute("UPDATE public.\"Order\" SET \"Status\" = 'FAILED' WHERE \"ID\" = %s", (order_id,))
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7000)

# To excecute: python3 -m uvicorn main:app --reload --port 7000
