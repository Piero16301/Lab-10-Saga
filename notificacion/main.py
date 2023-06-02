import urllib.request

import psycopg2
import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/notification")
async def notification(order_id):
    print(order_id)

    conn = psycopg2.connect(
        database="notifications", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )

    cur = conn.cursor()

    # 50 is the price of the seat -- this is hardcoded for educational purposes
    cur.execute("INSERT INTO public.\"notification\" (\"OrderID\", \"Type\", \"Status\") VALUES (%s, %s, %s)",
                (order_id, "EMAIL", "SUCCESS"))

    conn.commit()

    cur.close()
    conn.close()

    # DO NOT DELETE: simulating an error in the email service

    # Colocar Status = FAILED en la tabla de notificaciones
    conn = psycopg2.connect(
       database="notifications", user='postgres', password='postgres', host='127.0.0.1', port='5432'
    )
    cur = conn.cursor()
    cur.execute("UPDATE public.\"notification\" SET \"Status\" = 'FAILED' WHERE \"OrderID\" = %s", (order_id,))
    conn.commit()
    cur.close()
    conn.close()

    # Avisar a orquestador que hubo un error
    url = "http://localhost:7004/notification-rollback?order_id={}".format(order_id)
    response = urllib.request.urlopen(url)
    _ = response.read()

    raise Exception("Email service is down")

    # return {"Result": "Success"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7003)

# To excecute: python3 -m uvicorn main:app --reload --port 7000
