import mysql.connector
from mysql.connector import cursor
from decouple import config

drawId = "1"
numberOfWinners = 3
conn = None

def close_draw():
    sql = "UPDATE lottery_draws SET status = 'closed' WHERE id = " + drawId
    cursor.execute(sql)

def draw_winners():
    # Gewinner ziehen
    ticketIds = []
    userIds = []

    for i in range(numberOfWinners):
        if ticketIds and len(ticketIds) == 1:
            sql = "SELECT id, userId FROM lottery_tickets WHERE drawId = " + drawId + " AND id != " + str(ticketIds[0]) + " AND userId != " + str(userIds[0]) + " ORDER BY RAND() LIMIT 1"
        else:
            sql = ("SELECT id, userId FROM lottery_tickets WHERE drawId = " + drawId + (" AND id NOT IN {} AND userId NOT IN {}" if ticketIds else "") + " ORDER BY RAND() LIMIT 1").format(tuple(ticketIds), tuple(userIds))
        print(sql, ticketIds, userIds)
        cursor.execute(sql)
        result = cursor.fetchone()
        ticketIds.append(result[0])
        userIds.append(result[1])

    # Gewinnertickets in Datenbank vermerken
    sql = ("UPDATE lottery_tickets SET status = 'drawn' WHERE id IN {} AND drawId = " + drawId).format(tuple(ticketIds))
    print(sql)
    cursor.execute(sql)

def devalueTicketsExcept():
    sql = "UPDATE lottery_tickets SET status = 'closed' WHERE drawId = " + drawId + " AND status != 'drawn'"
    print(sql)
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
        print("Fehler: Ziehung wurde bereits ausgewertet oder existiert nicht")
        quit()

    close_draw()
    draw_winners()
    devalueTicketsExcept()

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
