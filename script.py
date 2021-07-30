import mysql.connector
from mysql.connector import cursor
from decouple import config

drawId = "1"
conn = None

def close_draw():
    sql = "UPDATE lottery_draws SET status = 'closed' WHERE id = " + drawId
    cursor.execute(sql)

def draw_winner():
    # Gewinner ziehen
    sql = "SELECT id, ticketNo FROM lottery_tickets WHERE drawId =" + drawId + " ORDER BY RAND() LIMIT 1"
    cursor.execute(sql)
    result = cursor.fetchone()
    id = result[0]

    # Gewinnerticket in Datenbank vermerken
    sql = "UPDATE lottery_tickets SET status = 'drawn' WHERE id = " + str(id)
    cursor.execute(sql)

    return id

def devalueTicketsExcept(winnerTicketId):
    sql = "UPDATE lottery_tickets SET status = 'closed' WHERE id != " + str(winnerTicketId)
    cursor.execute(sql)

try:
    conn = mysql.connector.connect(
    host=config("DB_HOST"),
    user=config("DB_USER"),
    password=config("DB_PASSWORD"),
    database=config("DB_NAME")
    )

    cursor = conn.cursor()

    sql = "SELECT * FROM lottery_draws WHERE status = 'open' AND id = " + str(drawId)
    cursor.execute(sql)
    result = cursor.fetchone()

    if result is None:
        print("Fehler: Ziehung wurde bereits ausgewertet")
        quit()

    close_draw()
    ticketId = draw_winner()
    devalueTicketsExcept(ticketId)

    conn.commit()
    print("Ziehung erfolgreich ausgewertet")

except mysql.connector.Error as error:
    print("Failed to update record to database rollback: {}".format(error))
    # reverting changes because of exception
    conn.rollback()
finally:
    # closing database connection.
    if conn.is_connected():
        cursor.close()
        conn.close()
