import urllib.request

import psycopg2
import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/seats")
async def seats(order_id, n_seats):
    print(order_id, n_seats)

    conn = psycopg2.connect(
        database="seats", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )

    cur = conn.cursor()

    # 50 is the price of the seat -- this is hardcoded for educational purposes
    cur.execute("INSERT INTO public.\"Seat\" (\"OrderID\", \"Total\", \"Status\") VALUES (%s, %s, %s)",
                (order_id, n_seats, "SUCCESS"))

    conn.commit()

    cur.close()
    conn.close()

    # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
    try:
        send_seats(order_id)
        return {"Result": "Success"}
    except Exception as e:
        # Avisar a orquestador que hubo un error
        url = "http://localhost:7004/seats-rollback?order_id={}".format(order_id)
        response = urllib.request.urlopen(url)
        _ = response.read()

        raise Exception("Email service is down")


@app.get("/seats-rollback")
async def seats_rollback(order_id):
    # Colocar Status = FAILED en la tabla de asientos
    conn = psycopg2.connect(
        database="seats", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )
    cur = conn.cursor()
    cur.execute("UPDATE public.\"Seat\" SET \"Status\" = 'FAILED' WHERE \"OrderID\" = %s", (order_id,))
    conn.commit()
    cur.close()
    conn.close()


def send_seats(order_id):
    print(order_id)
    url = "http://localhost:7003/notification?order_id={}".format(order_id)
    response = urllib.request.urlopen(url)
    data = response.read()
    print(data)


if __name__ == "__main__":
    uvicorn.run(app, host='127.0.0.1', port=7002)

# To excecute: python3 -m uvicorn main:app --reload --port 7000
