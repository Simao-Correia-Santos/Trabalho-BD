##
## =============================================
## ============== Bases de Dados ===============
## ============== LEI  2022/2023 ===============
## =============================================
## ================ Projeto ====================
## =============================================
## =============================================
## === Department of Informatics Engineering ===
## =========== University of Coimbra ===========
## =============================================
##
## Authors:
##   Gabriel Suzana Ferreira
##   Simão Correia Santos
##   Francisco José Coelho
##   University of Coimbra


import flask
import logging
import psycopg2
import jwt
import bcrypt
from datetime import datetime
import pytz

secret_token_key = "chave_mestra"
salt = bcrypt.gensalt()
app = flask.Flask(__name__)

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}


##########################################################
## DATABASE ACCESS
##########################################################

#DONT DO THIS!!!!!!!!!!!!!!
#SAVE THAT INFORMATION ON A FILE
#CHANGE USERNAME AND PASSOWORD TO SOMETHING LESS OBVIOUS
def db_connection():
    db = psycopg2.connect(
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
        database='dbproj'
    )

    return db


##########################################################
## ENDPOINTS
##########################################################


def isConsumer(header_token, cur, conn):
    query = 'SELECT consumer.person_user_id FROM person, consumer WHERE consumer.person_user_id = person.user_id AND %s = person.username'
    token = jwt.decode(header_token, secret_token_key, algorithms=['HS256'])
    username_token = token.get('username')

    try:
        cur.execute(query, (username_token,))
        conn.commit()
        result = cur.fetchone()[0]
        print(result)
        if result is not None:
            return result
        else:
            return None

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()


# FUNÇÃO PARA VERIFICAR SE O TOKEN PERTENCE A UM ADMIN LOGGADO
def isAdmin(header_token, cur, conn):
    query = 'SELECT admin.person_user_id FROM person, admin WHERE admin.person_user_id = person.user_id AND %s = person.username'
    token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
    username_token = token.get('username')

    try:
        cur.execute(query, (username_token,))
        conn.commit()
        result = cur.fetchone()[0]
        print(result)
        if result is not None:
            return True
        else:
            return False

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()


def isArtist(header_token, cur, conn):
    query = 'SELECT p.username FROM person p INNER JOIN artist a ON p.user_id = a.person_user_id WHERE p.username = %s;'
    token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
    username_token = token.get('username')

    try:
        cur.execute(query, (username_token,))
        result = cur.fetchone()
        print(result)
        conn.commit()

        if result is not None:
            return True
        else:
            return False

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()


def get_date():
    # Define timezone
    paris_tz = pytz.timezone('Europe/Paris')

    # Get current time in Paris timezone
    date_now = datetime.now(paris_tz)

    # Format the date
    formatted_date = date_now.strftime('%Y-%m-%dT%H:%M:%S')

    return formatted_date


# USER_REGISTRATION
@app.route('/dbproj/person/', methods=['POST'])
def user_registration():
    payload = flask.request.get_json()
    query_insert_person = 'INSERT INTO person (username, email, password, address, genre, age) values (%s, %s, %s, %s, %s, %s) returning user_id;'
    query_insert_consumer = 'INSERT INTO consumer (subscription_sub_id, person_user_id) values (%s, %s);'
    query_insert_artist = 'INSERT INTO artist (artistic_name, person_user_id) values (%s, %s);'

    if payload['username'] is None or payload['password'] is None or payload['email'] is None or payload[
        'address'] is None or payload['age'] is None:
        response = {'status': StatusCodes['api_error'], 'errors': 'Missing parameters'}
        return flask.jsonify(response)

    conn = db_connection()
    cur = conn.cursor()
    try:
        # Codificamos a password segundo uma função de hash
        hash_password = bcrypt.hashpw(payload['password'].encode('utf-8'), salt)
        # Obtemos o admin_token passado no header
        header_token = flask.request.headers.get('Authorization')
        # print(header_token)

        if header_token == '':
            # SIGNIFICA QUE ESTAMOS A ADICIONAR UM USER NORMAL
            values = (payload['username'], payload['email'], hash_password.decode('utf-8'), payload['address'], payload['genre'],
                      int(payload['age']))
            # After inserting into the "person" table
            cur.execute(query_insert_person, values)
            conn.commit()
            user_id = cur.fetchone()[0]
            ## insert in consumer table
            values = (None, user_id)
            cur.execute(query_insert_consumer, values)
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': f'Inserted user {user_id}'}

        else:
            # SIGNIFICA QUE ESTAMOS A ADICIONAR UM ARTISTA, POIS TEMOS UM ADMIN TOKEN A SER PASSADO
            if payload['artistic_name'] is None:
                response = {'status': StatusCodes['api_error'], 'errors': 'Missing parameters'}
                return flask.jsonify(response)

            if isAdmin(header_token, cur, conn) == 0:
                response = {'status': StatusCodes['api_error'], 'errors': 'Unauthorized Token'}
                return flask.jsonify(response)

            else:

                values = (
                    payload['username'], payload['email'], hash_password.decode('utf-8'), payload['address'], payload['genre'],
                    int(payload['age']))
                cur.execute(query_insert_person, values)
                conn.commit()
                user_id = cur.fetchone()[0]

                ## insert in artist table
                values = (payload['artistic_name'], user_id)
                cur.execute(query_insert_artist, values)
                conn.commit()
                response = {'status': StatusCodes['success'], 'results': f'Inserted user {user_id}'}

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/person/', methods=['PUT'])
def user_authentication():
    conn = db_connection()
    cur = conn.cursor()
    payload = flask.request.get_json()
    query_check = 'SELECT password FROM person WHERE username = %s;'

    if 'username' not in payload or 'password' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'Missing parameters'}
        return flask.jsonify(response)

    try:
        cur.execute(query_check, (payload['username'],))
        password_bd = cur.fetchone()
        if password_bd is not None:
            if bcrypt.checkpw(payload['password'].encode('utf-8'), password_bd[0].encode('utf-8')):
                generated_token = jwt.encode({"username": payload['username']}, secret_token_key, algorithm="HS256")
                response = {'status': StatusCodes['success'], 'results': generated_token}
            else:
                response = {'status': StatusCodes['api_error'], 'results': 'Invalid password'}

        else:
            response = {'status': StatusCodes['api_error'], 'results': 'Invalid username'}

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/music/', methods=['POST'])
def add_song():
    payload = flask.request.get_json()
    statement = 'INSERT INTO song(release_date, title, genre, duration, publisher_publisher_id) values (%s, %s, %s, %s, %s) RETURNING ismn'
    query_attach_song_to_artist = 'INSERT INTO song_artist(song_ismn, artist_person_user_id) values (%s, %s)'
    values_statement = (
        payload['release_date'], payload['title'], payload['genre'], payload['duration'], payload['publisher_id'])
    conn = db_connection()
    cur = conn.cursor()

    # Validate payload fields
    required_fields = ['release_date', 'title', 'genre', 'duration', 'duration', 'publisher_id']
    for field in required_fields:
        if field not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
            return flask.jsonify(response)

    # Insert the song and retrieve the generated song_id
    try:
        header_token = flask.request.headers.get('Authorization')

        if isArtist(header_token, cur, conn):
            query_find_artist_id = 'SELECT artist.person_user_id FROM person, artist WHERE person.user_id = artist.person_user_id AND %s = person.username;'
            token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
            username_token = token.get('username')

            ## vai buscar o person_user_id do artista
            cur.execute(query_find_artist_id, (username_token,))
            # Commit the transaction
            conn.commit()
            artist_user_id = cur.fetchone()

            ## adicionou a musica à tabela "song"
            cur.execute(statement, values_statement)
            ismn = cur.fetchone()[0]
            # Commit the transaction
            conn.commit()

            values_attach = (ismn, artist_user_id)
            ## e agora vamos adicionar à tabela music_record_label_artist os valores que identificam que certo artista está associado a uma musica
            cur.execute(query_attach_song_to_artist, values_attach)
            # Commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': ismn}
        else:
            response = {'status': StatusCodes['api_error'], 'results': f'token not authorized'}
            return flask.jsonify(response)

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/music/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/album/', methods=['POST'])
def add_album():
    payload = flask.request.get_json()

    ##adicionar o album
    query_insert_album = 'INSERT INTO album(title, release_date, publisher_publisher_id, artist_person_user_id) values (%s, %s, %s, %s) RETURNING album_id'
    query_find_album_title_existence = 'SELECT title FROM album WHERE title = %s'
    query_attach_song_to_artist = 'INSERT INTO song_artist(song_ismn, artist_person_user_id) values (%s, %s)'

    conn = db_connection()
    cur = conn.cursor()

    # Validate payload fields
    required_fields = ['title', 'release_date', 'publisher_id', 'songs']
    for field in required_fields:
        if field not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
            return flask.jsonify(response)
    # Insert the song and retrieve the generated song_id
    response = {'status': StatusCodes['internal_error'], 'errors': 'Unknown error occurred'}
    try:
        header_token = flask.request.headers.get('Authorization')
        if isArtist(header_token, cur, conn):
            query_find_artist_artistic_name = 'SELECT artist.person_user_id FROM person, artist WHERE person.user_id = artist.person_user_id AND %s = person.username;'
            token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
            username_token = token.get('username')

            ## vai buscar o person_user_id do artista
            cur.execute(query_find_artist_artistic_name, (username_token,))
            # Commit the transaction
            conn.commit()
            artist_user_id = cur.fetchone()[0]

            if artist_user_id is None:
                response = {'status': StatusCodes['api_error'], 'errors': 'Artist not found'}
                return flask.jsonify(response)

            ## vamos ver se o titulo já é igual a algum album
            cur.execute(query_find_album_title_existence, (payload['title'],))
            serch_album_title = cur.fetchone()
            if serch_album_title is not None:
                response = {'status': StatusCodes['internal_error'], 'errors': 'Album s title already exists'}
                return flask.jsonify(response)
            # insere o album na tabela
            values_insert_album = (
            payload['title'], payload['release_date'], int(payload['publisher_id']), artist_user_id)

            ## adicionou à tabela album
            cur.execute(query_insert_album, values_insert_album)
            album_id = cur.fetchone()[0]

            if payload['songs'] is not None:
                song_album = payload['songs']
                if song_album is None:
                    response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
                    return flask.jsonify(response)
                else:

                    for i in range(len(song_album)):
                        # caso seja uma musica ja existente
                        if isinstance(song_album[i], dict):
                            song_name = song_album[i]['song_name']
                            duration = song_album[i]['duration']
                            genre = song_album[i]['genre']
                            cur.execute(
                                'INSERT INTO song(release_date, title, genre, duration, publisher_publisher_id) values (%s, %s, %s, %s, %s) RETURNING ismn',
                                (payload['release_date'], song_name, genre, duration, payload['publisher_id'],))
                            ismn = cur.fetchone()[0]
                            cur.execute('INSERT INTO album_song(album_album_id, song_ismn) values (%s, %s)',
                                        (album_id, ismn,))
                            artists = song_album[i]['other_artists']
                            for j in range(len(artists)):
                                values = (ismn, artists[j])
                                cur.execute(query_attach_song_to_artist, values)
                        else:
                            cur.execute('INSERT INTO album_song(album_album_id, song_ismn) values (%s, %s)',
                                        (album_id, song_album[i],))
            # Commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': f'Inserted album_id {album_id}'}
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/album/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/song/', methods=['GET'])
def get_songs():
    keyword = flask.request.args.get('keyword')
    logger.info(f'GET dbproj/song/{keyword}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""SELECT song.title, song.genre, album.title, artist.artistic_name, playlist.name
                        FROM song
                        LEFT JOIN song_artist ON song.ismn = song_artist.song_ismn
                        LEFT JOIN artist ON song_artist.artist_person_user_id = artist.person_user_id
                        LEFT JOIN album_song ON song.ismn = album_song.song_ismn
                        LEFT JOIN album ON album_song.album_album_id = album.album_id
                        LEFT JOIN playlist_song ON song.ismn = playlist_song.song_ismn
                        LEFT JOIN playlist ON playlist_song.playlist_playlist_id = playlist.playlist_id
                        WHERE song.genre LIKE '%{palavra1}%' OR song.title LIKE '%{palavra2}%';""".format(
            palavra1=keyword, palavra2=keyword))
        rows = cur.fetchall()
        content = []

        for row in rows:
            result = {
                'song_title': row[0],
                'song_genre': row[1],
                'album_title': row[2],
                'artistic_name': row[3],
                'playlist_name': row[4],
            }
            content.append(result)
        response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /dbproj/song/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/artist_info/', methods=['GET'])
def get_detail_artist():
    artist_id = flask.request.args.get('artist_id')
    logger.info(f'GET dbproj/artist_info/{artist_id}')

    conn = db_connection()
    cur = conn.cursor()
    query = """SELECT artist.person_user_id, artist.artistic_name, 
                        album.album_id, album.title,
                        song.ismn, song.title,
                        playlist.playlist_id, playlist.name
                FROM artist
                    LEFT JOIN album ON artist.person_user_id = album.artist_person_user_id
                    LEFT JOIN album_song ON album.album_id = album_song.album_album_id
                    LEFT JOIN song ON album_song.song_ismn = song.ismn
                    LEFT JOIN playlist_song ON song.ismn = playlist_song.song_ismn
                    LEFT JOIN playlist ON playlist_song.playlist_playlist_id = playlist.playlist_id
                WHERE artist.person_user_id = %s;"""

    try:
        cur.execute(query, (artist_id,))
        rows = cur.fetchall()
        content = []

        for row in rows:
            result = {
                'artist_id': row[0],
                'artistic_name': row[1],
                'album_id': row[2],
                'album_title': row[3],
                'song_ismn': row[4],
                'song_title': row[5],
                'playlist_id': row[6],
                'playlist_name': row[7]
            }
            content.append(result)

        response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /dbproj/artist_info/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/subscription', methods=['POST'])
def subscribe_to_premium():
    logger.info('POST /dbproj/subscription')
    payload = flask.request.get_json()
    query_get_sub_value = 'SELECT cost FROM subscription_type WHERE type = %s'
    period = payload.get('period')
    cards_payload = payload.get('cards')
    cards = [int(card) for card in cards_payload]  # Convert string values to integers
    logger.info('period')

    # Validate payload fields
    if not period or not cards:
        response = {'status': StatusCodes['api_error'], 'results': 'Missing period or cards in payload'}
        return flask.jsonify(response)

    # Get the amount to pay
    conn = db_connection()
    cur = conn.cursor()

    logger.info('get sub_cost')
    cur.execute(query_get_sub_value, (period,))
    sub_cost_check = cur.fetchone()
    conn.commit()

    if sub_cost_check is None:
        response = {'status': StatusCodes['api_error'], 'results': 'Invalid subscription period'}
        return flask.jsonify(response)
    sub_cost = sub_cost_check[0]

    if period == "month":
        period_aux = '1 MONTH'
    elif period == "quarter":
        period_aux = '3 MONTHS'  # Changed to correct PSQL interval
    elif period == "semester":
        period_aux = '6 MONTHS'
    else:
        response = {'status': StatusCodes['api_error'], 'results': 'Invalid period'}
        return flask.jsonify(response)

    try:
        header_token = flask.request.headers.get('Authorization')
        query_find_user_id = 'SELECT user_id FROM person WHERE username = %s;'
        token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
        username_token = token.get('username')
        # VERIFICAMOS SE É UM CONSUMIDOR
        user_id = isConsumer(header_token, cur, conn)
        if user_id is None:
            response = {'status': StatusCodes['api_error'], 'errors': 'Unauthorized Token'}
            return flask.jsonify(response)
        else:
            ## vai buscar o user_id da pessoa que quer subscrever
            cur.execute(query_find_user_id, (username_token,))
            user_id = cur.fetchone()[0]
            print("user_id %s", user_id)
            # Commit the transaction
            conn.commit()

            # Check if the consumer has an active subscription
            query_get_isactive = 'SELECT subscription.sub_id, subscription.isactive, subscription.deadline FROM consumer JOIN subscription ON consumer.subscription_sub_id = subscription.sub_id WHERE consumer.person_user_id = %s;'
            cur.execute(query_get_isactive, (user_id,))
            result = cur.fetchone()
            print("result %s", result)
            if result is not None:
                sub_id = result[0]
                is_active = result[1]
            else:
                sub_id = None
                is_active = None
            # Commit the transaction
            conn.commit()

            if is_active is None:
                # Validate prepaid_card_card_id values
                for card_id in cards:
                    cur.execute("SELECT COUNT(*) FROM prepaid_card WHERE card_id = %s;", (card_id,))
                    count = cur.fetchone()[0]
                    if count == 0:
                        response = {'status': StatusCodes['api_error'],
                                    'results': f"Invalid prepaid card ID: {card_id}"}
                        return flask.jsonify(response)

                # Insert new subscription and get sub_id
                query_insert_subscription = "INSERT INTO subscription (isactive, deadline, subscription_type_type) VALUES (false, date_trunc('second', NOW() + INTERVAL %s), %s) RETURNING sub_id;"
                cur.execute(query_insert_subscription, (period_aux, period))
                sub_id = cur.fetchone()[0]

                # Update consumer subscription_id
                query_update_consumer = 'UPDATE consumer SET subscription_sub_id = %s WHERE person_user_id = %s;'
                cur.execute(query_update_consumer, (sub_id, user_id))

                # Check if any prepaid card is already associated with a different user
                query_check_association = """
                    SELECT pcc.prepaid_card_card_id
                    FROM prepaid_card_consumer AS pcc
                    WHERE pcc.prepaid_card_card_id = ANY(%s::bigint[])
                        AND pcc.consumer_person_user_id <> %s
                    """
                cur.execute(query_check_association, (cards, user_id))
                conflicting_cards = [card_id for (card_id,) in cur.fetchall()]
                if conflicting_cards:
                    response = {'status': StatusCodes['api_error'],
                                'results': 'Conflict: Prepaid card already assigned to a different user'}
                    return flask.jsonify(response)

                # Insert prepaid_card_consumer records
                query_insert_prepaid_card_consumer = """
                    INSERT INTO prepaid_card_consumer (prepaid_card_card_id, consumer_person_user_id)
                    SELECT pc.card_id, %s
                    FROM prepaid_card AS pc
                    WHERE pc.card_id = ANY(%s::bigint[])
                        AND pc.card_id NOT IN (
                            SELECT pcc.prepaid_card_card_id
                            FROM prepaid_card_consumer AS pcc
                            WHERE pcc.consumer_person_user_id = %s
                        )
                """
                cur.execute(query_insert_prepaid_card_consumer, (user_id, cards, user_id))

                execute_procedure_transaction = 'CALL process_transaction(%s::NUMERIC, %s::BIGINT, %s::BIGINT, %s::BIGINT[]);'
                cur.execute(execute_procedure_transaction, (sub_cost, user_id, sub_id, cards))
                print(sub_cost)

                query_set_to_active_sub = ('UPDATE subscription SET isactive = TRUE WHERE sub_id = %s;')
                cur.execute(query_set_to_active_sub, (sub_id,))
                # Commit the transaction
                conn.commit()
                # Check if any card had insufficient balance
                response = {'status': StatusCodes['api_error'],
                            'results': 'Transaction succeded. Subscription is now active.'}
                return flask.jsonify(response)
            else:
                # Validate prepaid_card_card_id values
                for card_id in cards:
                    cur.execute("SELECT COUNT(*) FROM prepaid_card WHERE card_id = %s;", (card_id,))
                    count = cur.fetchone()[0]
                    if count == 0:
                        response = {'status': StatusCodes['api_error'],
                                    'results': f"Invalid prepaid card ID: {card_id}"}
                        return flask.jsonify(response)

                # Check if any prepaid card is already associated with a different user
                query_check_association = """
                                   SELECT pcc.prepaid_card_card_id
                                   FROM prepaid_card_consumer AS pcc
                                   WHERE pcc.prepaid_card_card_id = ANY(%s::bigint[])
                                       AND pcc.consumer_person_user_id <> %s
                                   """
                cur.execute(query_check_association, (cards, user_id))
                conflicting_cards = [card_id for (card_id,) in cur.fetchall()]
                if conflicting_cards:
                    response = {'status': StatusCodes['api_error'],
                                'results': 'Conflict: Prepaid card already assigned to a different user'}
                    return flask.jsonify(response)

                # Insert prepaid_card_consumer records
                query_insert_prepaid_card_consumer = """
                                   INSERT INTO prepaid_card_consumer (prepaid_card_card_id, consumer_person_user_id)
                                   SELECT pc.card_id, %s
                                   FROM prepaid_card AS pc
                                   WHERE pc.card_id = ANY(%s::bigint[])
                                       AND pc.card_id NOT IN (
                                           SELECT pcc.prepaid_card_card_id
                                           FROM prepaid_card_consumer AS pcc
                                           WHERE pcc.consumer_person_user_id = %s
                                       )
                               """
                cur.execute(query_insert_prepaid_card_consumer, (user_id, cards, user_id))

                # Process transaction
                execute_procedure_transaction = 'CALL process_transaction(%s::NUMERIC, %s::BIGINT, %s::BIGINT, %s::BIGINT[]);'
                cur.execute(execute_procedure_transaction, (sub_cost, user_id, sub_id, cards))

                # Extend the existing subscription
                query_extend_subscription = """
                                            UPDATE subscription 
                                            SET deadline = deadline + INTERVAL %s
                                            WHERE sub_id = %s;
                                            """
                cur.execute(query_extend_subscription, (period_aux, sub_id))

                conn.commit()

                response = {'status': StatusCodes['api_error'],
                            'results': 'Transaction succeded. Subscription extended.'}
                return flask.jsonify(response)

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/subscription - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/playlist/', methods=['POST'])
def create_playlist():
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()

    query_inserir_playlist = 'INSERT INTO playlist (name, isPublic, consumer_person_user_id) values (%s, %s, %s) RETURNING playlist_id'

    # Initialize response with an error status
    response = {'status': StatusCodes['internal_error'], 'results': 'An error occurred'}

    # Validate payload fields
    required_fields = ['name', 'isPublic', 'songs']
    for field in required_fields:
        if field not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
            return flask.jsonify(response)

    try:
        header_token = flask.request.headers.get('Authorization')

        # VERIFICAMOS SE É UM CONSUMIDOR
        user_id = isConsumer(header_token, cur, conn)
        if user_id is None:
            response = {'status': StatusCodes['api_error'], 'results': 'Unauthorized token'}
            return flask.jsonify(response)

        cur.execute('SELECT subscription_sub_id from consumer where consumer.person_user_id = %s', (user_id,))
        has_sub_id = cur.fetchone()[0]

        if has_sub_id is None:
            response = {'status': StatusCodes['api_error'], 'results': f'token not authorized (not premium)'}
            return flask.jsonify(response)

        cur.execute(
            'SELECT s.isactive FROM subscription s INNER JOIN consumer c ON s.sub_id = c.subscription_sub_id WHERE c.person_user_id = %s',
            (user_id,))
        subscription_status = cur.fetchone()[0]

        if subscription_status is not None:
            values_statement = (payload['name'], payload['isPublic'], user_id)
            cur.execute(query_inserir_playlist, values_statement)
            # Commit the transaction
            conn.commit()
            playlist_id = cur.fetchone()[0]

            if payload['songs'] is not None:
                songs = payload['songs']
                if songs is None:
                    response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
                    return flask.jsonify(response)
                else:

                    for i in range(len(songs)):
                        cur.execute('SELECT * FROM song WHERE ismn = %s', (songs[i],))
                        # Commit the transaction
                        conn.commit()
                        song_check = cur.fetchone()
                        if song_check is None:

                            response = {'status': StatusCodes['api_error'], 'results': f'{songs[i]} does not exist'}
                            return flask.jsonify(response)
                        else:
                            cur.execute('INSERT INTO playlist_song(playlist_playlist_id, song_ismn) values (%s, %s)',
                                        (playlist_id, songs[i],))
                            # Commit the transaction
                            conn.commit()
            response = {'status': StatusCodes['success'], 'results': playlist_id}
        else:
            response = {'status': StatusCodes['api_error'], 'results': f'token not authorized (not premium)'}
            return flask.jsonify(response)

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/album/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/', methods=['PUT'])
def play_song():
    song_id = flask.request.args.get('song_id')
    conn = db_connection()

    cur = conn.cursor()
    check_song_query = 'SELECT ismn FROM song where ismn = %s;'
    execute_procedure = 'call play_song(%s, %s)'

    try:
        header_token = flask.request.headers.get('Authorization')
        # VERIFICAMOS SE É UM CONSUMIDOR
        user_id = isConsumer(header_token, cur, conn)
        if user_id is None:
            response = {'status': StatusCodes['api_error'], 'results': 'Unauthorized token'}
            return flask.jsonify(response)

        # VERIFICAMOS SE O SONG ID RECEBIDO EXISTE NA BASE DE DADOS
        cur.execute(check_song_query, (song_id,))
        conn.commit()
        result = cur.fetchone()

        if result is not None:
            cur.execute(execute_procedure, (song_id, user_id,))
            conn.commit()
            response = {'status': StatusCodes['success']}

        else:
            response = {'status': StatusCodes['api_error'], 'results': 'Song Id not found'}
            return flask.jsonify(response)

    except (Exception, psycopg2.DatabaseError) as e:
        response = {'status': StatusCodes['internal_error'], 'errors': str(e)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/card', methods=['POST'])
def generate_prepaid_cards():
    logger.info('POST /dbproj/card')
    payload = flask.request.get_json()
    number_cards = payload.get('number_cards')
    card_price = payload.get('card_price')
    deadline = payload.get('deadline')

    # Validate payload fields
    if not number_cards or not card_price:
        response = {'status': StatusCodes['api_error'], 'results': 'Missing number_cards or card_price in payload'}
        return flask.jsonify(response)

    conn = db_connection()
    cur = conn.cursor()

    query_check_card_price = ('SELECT * FROM prepaid_card_cost WHERE cost = %s;')
    cur.execute(query_check_card_price, (card_price,))
    # Commit the transaction
    conn.commit()
    costs = cur.fetchone()

    if costs is None:
        response = {'status': StatusCodes['api_error'], 'results': 'Invalid card price'}
        return flask.jsonify(response)

    try:
        header_token = flask.request.headers.get('Authorization')

        if isAdmin(header_token, cur, conn) == 0:
            response = {'status': StatusCodes['api_error'], 'errors': 'Unauthorized Token'}
            return flask.jsonify(response)

        query_find_admin_id = 'SELECT admin.person_user_id FROM person, admin WHERE person.user_id = admin.person_user_id AND %s = person.username;'
        token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
        username_token = token.get('username')

        cur.execute(query_find_admin_id, (username_token,))
        conn.commit()
        admin_id = cur.fetchone()[0]

        card_ids = []
        for _ in range(number_cards):
            # Generate a prepaid card
            cur.execute(
                'INSERT INTO prepaid_card (deadline,remaining_balance, admin_person_user_id) VALUES (%s , %s, %s) RETURNING card_id',
                (deadline, card_price, admin_id))
            # Commit the transaction
            conn.commit()

            card_id = cur.fetchone()[0]
            card_ids.append(card_id)

        response = {'status': StatusCodes['success'], 'results': card_ids}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/card - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/comments/', methods=['POST'])
def leave_comment():
    payload = flask.request.get_json()
    conn = db_connection()
    cur = conn.cursor()
    song_id = flask.request.args.get('song_id')
    comment_log_comment_id = flask.request.args.get('comment_log_comment_id')

    query_inserir_playlist = 'INSERT INTO comment_log (comment_date,text, comment_log_comment_id, consumer_person_user_id, song_ismn) values (%s, %s, %s, %s, %s) RETURNING comment_id;'

    # Initialize response with an error status
    response = {'status': StatusCodes['internal_error'], 'results': 'An error occurred'}

    # Validate payload fields
    required_fields = ['comment_date', 'text']
    for field in required_fields:
        if field not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{field} value not in payload'}
            return flask.jsonify(response)

    try:
        header_token = flask.request.headers.get('Authorization')

        # verificar se o user e consumer
        if isConsumer(header_token, cur, conn):
            # vai desdocificar o token para ter o username
            token = jwt.decode(header_token, secret_token_key, algorithms=["HS256"])
            username_token = token.get('username')

            cur.execute('select user_id from person where %s = username', (username_token,))
            id_token = cur.fetchone()[0]

            if comment_log_comment_id is None:
                cur.execute(query_inserir_playlist, (payload['comment_date'], payload['text'], -1, id_token, song_id))
            elif int(comment_log_comment_id) > 0:
                cur.execute('select comment_id from comment_log where comment_id = %s', (comment_log_comment_id,))
                comment_exists = cur.fetchone()
                if comment_exists:
                    cur.execute(query_inserir_playlist, (
                    payload['comment_date'], payload['text'], int(comment_log_comment_id), id_token, song_id))
                else:
                    response = {'status': StatusCodes['api_error'], 'results': 'Comment not found'}
                    return flask.jsonify(response)

                # Commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': 'comment inserted!'}
        else:
            response = {'status': StatusCodes['api_error'], 'results': f'token not authorized'}
            return flask.jsonify(response)

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/comment/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # An error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/report/', methods=['GET'])
def get_monthly_report():
    keyword = flask.request.args.get('year-month')
    logger.info(f'GET dbproj/report/{keyword}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""SELECT 
                            TO_CHAR(music_log.log_date, 'YYYY Month') AS month,
                            song.genre,
                            COUNT(DISTINCT music_log.song_ismn) AS unique_songs_played
                        FROM 
                            music_log
                        JOIN 
                            song ON music_log.song_ismn = song.ismn
                        WHERE 
                            TO_CHAR(music_log.log_date, 'YYYY-MM') = %s AND
                            music_log.log_date >= NOW() - INTERVAL '1 YEAR'
                        GROUP BY 
                            month, song.genre
                        ORDER BY 
                            month DESC, unique_songs_played DESC;""", (keyword,))
        rows = cur.fetchall()
        content = []
        for row in rows:
            content.append({"genre": row[1], "month": row[0].strip(), "playbacks": row[2]})
        response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /dbproj/report/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


if __name__ == '__main__':
    # set up logging
    logging.basicConfig(filename='log_file.log')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s', '%H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    host = '127.0.0.1'
    port = 8080
    app.run(host=host, debug=True, threaded=True, port=port)
    logger.info(f'API v1.0 online: http://{host}:{port}')
