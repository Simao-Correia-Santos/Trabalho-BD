CREATE DATABASE dbproj
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


CREATE TABLE consumer (
	subscription_sub_id BIGINT,
	person_user_id	 BIGINT,
	PRIMARY KEY(person_user_id)
);

CREATE TABLE artist (
	artistic_name	 VARCHAR(512) NOT NULL,
	person_user_id BIGINT,
	PRIMARY KEY(person_user_id)
);

CREATE TABLE person (
	user_id	 BIGSERIAL,
	username VARCHAR(512) NOT NULL,
	email	 VARCHAR(512) NOT NULL,
	password VARCHAR(512) NOT NULL,
	address	 VARCHAR(512) NOT NULL,
	genre	 VARCHAR(512) NOT NULL,
	age	 INTEGER NOT NULL,
	PRIMARY KEY(user_id)
);



CREATE TABLE song (
	ismn			 BIGSERIAL,
	release_date		 TIMESTAMP NOT NULL,
	title			 VARCHAR(512) NOT NULL,
	genre			 VARCHAR(512) NOT NULL,
	duration		 INTEGER NOT NULL,
	publisher_publisher_id BIGINT NOT NULL,
	PRIMARY KEY(ismn)
);

CREATE TABLE playlist (
	playlist_id		 BIGSERIAL,
	name			 VARCHAR(512) NOT NULL,
	ispublic		 BOOL NOT NULL,
	consumer_person_user_id BIGINT NOT NULL,
	PRIMARY KEY(playlist_id)
);

CREATE TABLE album (
	album_id		 BIGSERIAL,
	title			 VARCHAR(512) NOT NULL,
	release_date		 TIMESTAMP NOT NULL,
	publisher_publisher_id BIGINT NOT NULL,
	artist_person_user_id	 BIGINT NOT NULL,
	PRIMARY KEY(album_id)
);

CREATE TABLE prepaid_card (
	card_id		 BIGSERIAL,
	deadline		 TIMESTAMP NOT NULL,
	remaining_balance	 NUMERIC(8,2) NOT NULL,
	admin_person_user_id BIGINT NOT NULL,
	PRIMARY KEY(card_id)
);

CREATE TABLE prepaid_card_cost (
	cost INTEGER NOT NULL,
	PRIMARY KEY(cost)
);

CREATE TABLE subscription_type (
	type VARCHAR(512),
	cost INTEGER NOT NULL,
	PRIMARY KEY(type)
);

CREATE TABLE comment_log (
	comment_id		 BIGSERIAL,
	comment_date		 TIMESTAMP NOT NULL,
	text			 VARCHAR(512) NOT NULL,
	comment_log_comment_id	 BIGINT NOT NULL,
	consumer_person_user_id BIGINT NOT NULL,
	song_ismn		 BIGINT NOT NULL,
	PRIMARY KEY(comment_id)
);

CREATE TABLE publisher (
	publisher_id BIGSERIAL,
	name	 VARCHAR(512) NOT NULL,
	PRIMARY KEY(publisher_id)
);

CREATE TABLE subscription (
	sub_id		 BIGSERIAL,
	deadline		 TIMESTAMP NOT NULL,
	isactive		 BOOL NOT NULL,
	subscription_type_type VARCHAR(512) NOT NULL,
	PRIMARY KEY(sub_id)
);

CREATE TABLE music_log (
	log_date		 DATE NOT NULL,
	daily_counter		 INTEGER NOT NULL,
	song_ismn		 BIGINT,
	consumer_person_user_id BIGINT,
	PRIMARY KEY(log_date,song_ismn,consumer_person_user_id)
);

CREATE TABLE admin (
	person_user_id BIGINT,
	PRIMARY KEY(person_user_id)
);

CREATE TABLE transacao (
	trans_id		 BIGSERIAL,
	data		 TIMESTAMP,
	amount_paid		 NUMERIC(8,2) NOT NULL,
	subscription_sub_id	 BIGINT NOT NULL,
	prepaid_card_card_id BIGINT NOT NULL,
	PRIMARY KEY(trans_id)
);

CREATE TABLE top10 (
	song_ismn		 BIGINT,
	consumer_person_user_id BIGINT,
	PRIMARY KEY(song_ismn,consumer_person_user_id)
);

CREATE TABLE prepaid_card_consumer (
	prepaid_card_card_id	 BIGINT,
	consumer_person_user_id BIGINT NOT NULL,
	PRIMARY KEY(prepaid_card_card_id)
);

CREATE TABLE album_song (
	album_album_id BIGINT,
	song_ismn	 BIGINT NOT NULL,
	PRIMARY KEY(album_album_id, song_ismn)
);

CREATE TABLE playlist_song (
	playlist_playlist_id BIGINT,
	song_ismn		 BIGINT,
	PRIMARY KEY(playlist_playlist_id,song_ismn)
);

CREATE TABLE song_artist (
	song_ismn		 BIGINT,
	artist_person_user_id BIGINT,
	PRIMARY KEY(song_ismn,artist_person_user_id)
);

ALTER TABLE consumer ADD CONSTRAINT consumer_fk1 FOREIGN KEY (subscription_sub_id) REFERENCES subscription(sub_id);
ALTER TABLE consumer ADD CONSTRAINT consumer_fk2 FOREIGN KEY (person_user_id) REFERENCES person(user_id);
ALTER TABLE artist ADD CONSTRAINT artist_fk1 FOREIGN KEY (person_user_id) REFERENCES person(user_id);
ALTER TABLE person ADD UNIQUE (username, email);
ALTER TABLE song ADD CONSTRAINT song_fk1 FOREIGN KEY (publisher_publisher_id) REFERENCES publisher(publisher_id);
ALTER TABLE playlist ADD CONSTRAINT playlist_fk1 FOREIGN KEY (consumer_person_user_id) REFERENCES consumer(person_user_id);
ALTER TABLE album ADD CONSTRAINT album_fk1 FOREIGN KEY (publisher_publisher_id) REFERENCES publisher(publisher_id);
ALTER TABLE album ADD CONSTRAINT album_fk2 FOREIGN KEY (artist_person_user_id) REFERENCES artist(person_user_id);
ALTER TABLE prepaid_card ADD CONSTRAINT prepaid_card_fk1 FOREIGN KEY (admin_person_user_id) REFERENCES admin(person_user_id);
ALTER TABLE comment_log ADD CONSTRAINT comment_log_fk2 FOREIGN KEY (consumer_person_user_id) REFERENCES consumer(person_user_id);
ALTER TABLE comment_log ADD CONSTRAINT comment_log_fk3 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE subscription ADD CONSTRAINT subscription_fk1 FOREIGN KEY (subscription_type_type) REFERENCES subscription_type(type);
ALTER TABLE music_log ADD CONSTRAINT music_log_fk1 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE music_log ADD CONSTRAINT music_log_fk2 FOREIGN KEY (consumer_person_user_id) REFERENCES consumer(person_user_id);
ALTER TABLE admin ADD CONSTRAINT admin_fk1 FOREIGN KEY (person_user_id) REFERENCES person(user_id);
ALTER TABLE transacao ADD CONSTRAINT transacao_fk1 FOREIGN KEY (subscription_sub_id) REFERENCES subscription(sub_id);
ALTER TABLE transacao ADD CONSTRAINT transacao_fk2 FOREIGN KEY (prepaid_card_card_id) REFERENCES prepaid_card(card_id);
ALTER TABLE top10 ADD CONSTRAINT top10_fk1 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE top10 ADD CONSTRAINT top10_fk2 FOREIGN KEY (consumer_person_user_id) REFERENCES consumer(person_user_id);
ALTER TABLE prepaid_card_consumer ADD CONSTRAINT prepaid_card_consumer_fk1 FOREIGN KEY (prepaid_card_card_id) REFERENCES prepaid_card(card_id);
ALTER TABLE prepaid_card_consumer ADD CONSTRAINT prepaid_card_consumer_fk2 FOREIGN KEY (consumer_person_user_id) REFERENCES consumer(person_user_id);
ALTER TABLE album_song ADD CONSTRAINT album_song_fk1 FOREIGN KEY (album_album_id) REFERENCES album(album_id);
ALTER TABLE album_song ADD CONSTRAINT album_song_fk2 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE playlist_song ADD CONSTRAINT playlist_song_fk1 FOREIGN KEY (playlist_playlist_id) REFERENCES playlist(playlist_id);
ALTER TABLE playlist_song ADD CONSTRAINT playlist_song_fk2 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE song_artist ADD CONSTRAINT song_artist_fk1 FOREIGN KEY (song_ismn) REFERENCES song(ismn);
ALTER TABLE song_artist ADD CONSTRAINT song_artist_fk2 FOREIGN KEY (artist_person_user_id) REFERENCES artist(person_user_id);



INSERT INTO public.subscription_type (type, cost) VALUES ('quarter', 21);
INSERT INTO public.subscription_type (type, cost) VALUES ('month', 7);
INSERT INTO public.subscription_type (type, cost) VALUES ('semester', 42);




INSERT INTO public.person (user_id, username, email, password, address, genre, age) VALUES (11, 'admin', 'admin', '$2b$12$mLoK4bGjjWOCsd8pGHjkpe2h9w9tSXGfoUzzhMJ83ctBtHMR53yQO', 'admin', 'male', 30);

INSERT INTO public.admin (person_user_id) VALUES (11);


INSERT INTO public.prepaid_card_cost (cost) VALUES (25);
INSERT INTO public.prepaid_card_cost (cost) VALUES (10);
INSERT INTO public.prepaid_card_cost (cost) VALUES (50);


INSERT INTO public.publisher (publisher_id, name) VALUES (2, 'Apple Music');
INSERT INTO public.publisher (publisher_id, name) VALUES (1, 'Sony');





CREATE OR REPLACE PROCEDURE process_transaction(
  p_cost NUMERIC(8,2),
  p_user_id person.user_id%type,
  p_sub_id subscription.sub_id%type,
  p_card_ids BIGINT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  current_card_id BIGINT;
  remaining_cost NUMERIC(8,2) := p_cost;
  card_balance NUMERIC(8,2);
BEGIN
    FOREACH current_card_id IN ARRAY p_card_ids
    LOOP
      -- Acquire an exclusive lock on the prepaid_card row
      SELECT remaining_balance INTO card_balance FROM prepaid_card WHERE card_id = current_card_id FOR UPDATE;

      IF card_balance >= remaining_cost THEN
        UPDATE prepaid_card SET remaining_balance = remaining_balance - remaining_cost WHERE card_id = current_card_id;
        RAISE NOTICE 'Card % had its balance reduced by %.2f. New balance: %.2f', current_card_id, remaining_cost, card_balance - remaining_cost;

        -- Inserimos na tabela de transações o registo da transação
        INSERT INTO transacao (data, subscription_sub_id, amount_paid, prepaid_card_card_id)
        VALUES (date_trunc('second', NOW()::timestamp), p_sub_id, remaining_cost, current_card_id);

        remaining_cost := 0;
        EXIT;
      ELSE
        UPDATE prepaid_card SET remaining_balance = 0 WHERE card_id = current_card_id;
        RAISE NOTICE 'Card % had its balance reduced by %.2f. New balance: 0', current_card_id, card_balance;

        -- Inserimos na tabela de transações o registo da transação
        INSERT INTO transacao (data, subscription_sub_id, amount_paid, prepaid_card_card_id)
        VALUES (date_trunc('second', NOW()::timestamp), p_sub_id, card_balance, current_card_id);

        remaining_cost := remaining_cost - card_balance;
      END IF;

      IF remaining_cost <= 0 THEN
        EXIT;
      END IF;
    END LOOP;

    IF remaining_cost > 0 THEN
      -- Damos rollback à transação caso não haja dinheiro suficiente nos cartões
      RAISE EXCEPTION 'Not enough balance in the cards to cover the cost of %.2f', remaining_cost;
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE play_song(p_song_id song.ISMN%type, p_user_id person.user_id%type)
LANGUAGE plpgsql
AS $$
BEGIN
		-- Acquire an exclusive lock on the top10 table
		LOCK TABLE top10 IN EXCLUSIVE MODE;

		-- Remove as musicas que estão na tabela top10 que não foram ouvidas nos ultimos 30 dias
		DELETE FROM top10
		WHERE song_ismn NOT IN (
			SELECT song_ismn
			FROM v_last30days
		);

		-- Acquire an exclusive lock on the music_log table
		LOCK TABLE music_log IN EXCLUSIVE MODE;

		-- Verificamos se já existe um log da musica para o user em questao no dia atual
		IF EXISTS (
			SELECT 1
			FROM music_log
			WHERE song_ismn = p_song_id
			  AND consumer_person_user_id = p_user_id
			  AND log_date = current_date
		) THEN
			-- Incrementamos o daily counter se já existir para o dia atual
			UPDATE music_log
			SET daily_counter = daily_counter + 1
			WHERE song_ismn = p_song_id
			  AND consumer_person_user_id = p_user_id
			  AND log_date = current_date;
		ELSE
			-- Ou inserimos um novo log
			INSERT INTO music_log (song_ismn, consumer_person_user_id, daily_counter, log_date)
			VALUES (p_song_id, p_user_id, 1, current_date);
		END IF;
END;
$$;

CREATE OR REPLACE FUNCTION p_1() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    current_songs INTEGER;
    least_played_song song.ismn%type;
BEGIN
	-- Iniciamos uma transação para podermos dar rollback caso haja algum problema
  	BEGIN
		-- Acquire an exclusive lock on the top10 table
		LOCK TABLE top10 IN EXCLUSIVE MODE;

		-- Conta o numero de songs que o user ja tem no seu top10
		SELECT COUNT(*) INTO current_songs FROM top10 WHERE consumer_person_user_id = NEW.consumer_person_user_id;

		-- Se existir espaço e a song nao existir na tabela
		IF current_songs < 10 AND NOT EXISTS (
			SELECT 1 FROM top10 WHERE song_ismn = NEW.song_ismn AND consumer_person_user_id = NEW.consumer_person_user_id
		) THEN
			-- inserimos a song na tabela
			INSERT INTO top10 (song_ismn, consumer_person_user_id) VALUES (NEW.song_ismn, NEW.consumer_person_user_id);

		-- Se a tabela top10 estiver já com as 10 songs do user e a song que estamos a verificar não está lá, vamos substituir pela song menos tocada no top 10 se a song que estamosa  verificar tiver sido tocada mais vezes
		ELSIF current_songs = 10 AND NOT EXISTS (
			SELECT 1 FROM top10 WHERE song_ismn = NEW.song_ismn AND consumer_person_user_id = NEW.consumer_person_user_id
		) THEN
			-- Acquire an exclusive lock on the v_last30days view
			LOCK TABLE v_last30days IN EXCLUSIVE MODE;

			-- Encontra a song menos tocada para eventualmente ser substituida
			SELECT song_ismn INTO least_played_song
			FROM v_last30days
			WHERE consumer_person_user_id = NEW.consumer_person_user_id
			ORDER BY sum_counter ASC
			LIMIT 1;

			-- Verificamos se a nova song foi tocada mais vezes
			IF (
				SELECT sum_counter
				FROM v_last30days
				WHERE song_ismn = NEW.song_ismn AND consumer_person_user_id = NEW.consumer_person_user_id
			) > (
				SELECT sum_counter
				FROM v_last30days
				WHERE song_ismn = least_played_song AND consumer_person_user_id = NEW.consumer_person_user_id
			) THEN
				-- Se sim, apagamos a menos tocada e inserimos a nova song
				DELETE FROM top10
				WHERE song_ismn = least_played_song AND consumer_person_user_id = NEW.consumer_person_user_id;

				INSERT INTO top10 (song_ismn, consumer_person_user_id)
				VALUES (NEW.song_ismn, NEW.consumer_person_user_id);
			END IF;
		END IF;

		RETURN NULL;
		COMMIT;
	EXCEPTION
    	-- Em caso de erro damos rollback à transação
    	WHEN OTHERS THEN
      		-- Rollback the transaction
      		ROLLBACK;
     		 -- Raise the error again to propagate it
     		RAISE;
	END;
END;
$$;

CREATE OR REPLACE VIEW v_last30days AS
SELECT song_ismn, consumer_person_user_id, SUM(daily_counter) AS sum_counter
FROM music_log
WHERE log_date >= current_date - interval '30 days'
GROUP BY song_ismn, consumer_person_user_id
ORDER BY sum_counter DESC;

create or replace trigger t_play_song
after insert or update on music_log
for each row
execute procedure p_1();


CREATE OR REPLACE FUNCTION update_card_balance()
RETURNS TRIGGER AS $$
BEGIN
		IF DATE(NEW.deadline) <= CURRENT_DATE THEN
			-- Acquire an exclusive lock on the prepaid_card row
			SELECT remaining_balance INTO NEW.remaining_balance FROM prepaid_card WHERE card_id = NEW.card_id FOR UPDATE;

			-- Update the remaining_balance column
			NEW.remaining_balance = -1; -- Define the value -1 to indicate that the card has expired
		END IF;
		RETURN NEW;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE TRIGGER trigger_update_card_balance
BEFORE INSERT OR UPDATE ON prepaid_card
FOR EACH ROW
EXECUTE FUNCTION update_card_balance();


CREATE OR REPLACE FUNCTION update_subscription_status()
RETURNS TRIGGER AS $$
BEGIN
		-- Acquire an exclusive lock on the subscription row
		PERFORM * FROM subscription WHERE sub_id = NEW.sub_id FOR UPDATE;

		-- Perform the necessary updates
		IF DATE(NEW.deadline) <= CURRENT_DATE THEN
			NEW.isactive := FALSE;
		END IF;

		RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER subscription_update_trigger
BEFORE INSERT OR UPDATE ON subscription
FOR EACH ROW
EXECUTE PROCEDURE update_subscription_status();
