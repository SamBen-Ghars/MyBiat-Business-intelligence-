postgres_create_query='''


        CREATE TABLE IF NOT EXISTS dim_region (
                id_region INT PRIMARY KEY NOT NULL,
                libelle_region VARCHAR(255),
                CONSTRAINT id_region_unique UNIQUE (id_region)
                );

        CREATE TABLE IF NOT EXISTS dim_zone (
                id_zone INT PRIMARY KEY NOT NULL,
                id_region INT REFERENCES dim_region(id_region) NULL ,
                libelle_zone VARCHAR(255),
                CONSTRAINT zone_unique_id_fk UNIQUE (id_zone)
                );

        CREATE TABLE IF NOT EXISTS dim_agence (
                id_agence VARCHAR(4) PRIMARY KEY NOT NULL,
                id_zone INT REFERENCES dim_zone(id_zone) NULL,
                id_region INT REFERENCES dim_region(id_region) NULL,
                libelle_agence VARCHAR(255) NULL,
                classe_agence INTEGER,
                CONSTRAINT agence_unique_id UNIQUE (id_agence)
                );

        CREATE TABLE IF NOT EXISTS dim_client (
                id_client INT PRIMARY KEY NOT NULL,
                nom VARCHAR(255),
                gsm VARCHAR(255),
                adresse_mail VARCHAR(255),
                CONSTRAINT client_unique_id
                UNIQUE (id_client)
                );

        CREATE TABLE IF NOT EXISTS dim_souscription (
                id_souscrit VARCHAR(255) PRIMARY KEY NOT NULL,
                id_client INT REFERENCES dim_client(id_client) NULL,
                nom VARCHAR(255) NULL,
                adresse_mail VARCHAR(255) NULL,
                gsm VARCHAR(255) NULL,
                phone_verified BOOLEAN NULL,
                update_password BOOLEAN NULL,
                date_souscrit TIME NULL,
                CONSTRAINT dim_souscrit_unique_id
                UNIQUE (id_souscrit)
                );

        CREATE TABLE IF NOT EXISTS dim_date_virement (
                id_date_vrmt VARCHAR(255) PRIMARY KEY NOT NULL,
                annees INT NULL,
                mois INT NULL,
                jours INT NULL,
                heures TIME NULL,
                CONSTRAINT dim_date_unique_id
                UNIQUE (id_date_vrmt)
                );

        CREATE TABLE IF NOT EXISTS dim_authentification (
                id_authen VARCHAR(255) PRIMARY KEY NOT NULL,
                id_date_auth VARCHAR(255) NULL,
                id_souscrit VARCHAR(255) NULL,
                canal VARCHAR(255) NULL,
                error VARCHAR(255) NULL,
                date_auth date NULL,
                CONSTRAINT dim_auth_unique_id
                UNIQUE (id_authen)
                );

        CREATE TABLE IF NOT EXISTS fact_conquete (
                id_conquete VARCHAR(255) PRIMARY KEY NOT NULL,
                id_client INT REFERENCES dim_client(id_client) NULL,
                conquete BIGINT NULL,
                CONSTRAINT fact_conquete_unique_id
                UNIQUE (id_conquete)
                );

        CREATE TABLE IF NOT EXISTS dim_date_chequier (
                id_date_cheq VARCHAR(255) PRIMARY KEY NOT NULL,
                date_demande TIME NULL,
                CONSTRAINT date_chequier_unique_id
                UNIQUE (id_date_cheq)
                );

        CREATE TABLE IF NOT EXISTS fact_chequier (
                id_chequier VARCHAR(255) PRIMARY KEY NOT NULL,
                id_client INT REFERENCES dim_client(id_client) NULL,
                id_date_cheq VARCHAR(255) REFERENCES
                dim_date_chequier(id_date_cheq) NULL,
                event_category VARCHAR(255) NULL,
                event_desc VARCHAR(255) NULL,
                date_demande TIME NULL,
                CONSTRAINT fact_chequier_unique_id
                UNIQUE (id_chequier)
                );

        CREATE TABLE IF NOT EXISTS fact_date_souscription (
                id_date_souscrit VARCHAR(255) PRIMARY KEY NOT NULL,
                id_souscrit VARCHAR(255) REFERENCES dim_souscription(id_souscrit) NULL,
                date_souscrit TIME NULL,
                CONSTRAINT fact_date_unique_id
                UNIQUE (id_date_souscrit)
                );

        CREATE TABLE IF NOT EXISTS fact_nb_client (
                id_nb_client VARCHAR(255) PRIMARY KEY NOT NULL,
                id_agence VARCHAR(4) REFERENCES dim_agence(id_agence) NULL,
                nb_client NUMERIC(38,0) NULL,
                CONSTRAINT fact_nb_client_unique_id
                UNIQUE (id_nb_client)
                );

        CREATE TABLE IF NOT EXISTS fact_nb_souscrit (
                id_nb_souscrit VARCHAR(255) PRIMARY KEY NOT NULL,
                id_agence VARCHAR(4) REFERENCES dim_agence(id_agence) NULL,
                nb_souscrit INT NULL,
                CONSTRAINT fact_nb_souscrit_unique_id
                UNIQUE (id_nb_souscrit)
                );

        CREATE TABLE IF NOT EXISTS fact_objectifs (
                id_objectif VARCHAR(255) PRIMARY KEY NOT NULL,
                id_agence VARCHAR(4) REFERENCES dim_agence(id_agence) NULL,
                id_zone INT REFERENCES dim_zone(id_zone) NULL,
                id_region INT REFERENCES dim_region(id_region) NULL,
                nb_souscrit_rls NUMERIC(38,0) NULL,
                nb_souscrit_obj NUMERIC(38,0) NULL,
                date_objectif TIME NULL,
                CONSTRAINT fact_objectifs_unique_id
                UNIQUE (id_objectif)
                );

        CREATE TABLE IF NOT EXISTS fact_types_access (
                id_type_access VARCHAR(255) PRIMARY KEY NOT NULL,
                id_client INT REFERENCES dim_client(id_client) NULL,
                id_types_access VARCHAR(255) NULL,
                CONSTRAINT fact_type_access_unique_id
                UNIQUE (id_type_access)
                );

        CREATE TABLE IF NOT EXISTS fact_segement (
                id_segement VARCHAR(255) PRIMARY KEY NOT NULL,
                id_client INT REFERENCES dim_client(id_client) NULL,
                id_agence VARCHAR(4) REFERENCES dim_agence(id_agence) NULL,
                segement VARCHAR(255) NULL,
                CONSTRAINT fact_segement_unique_id
                UNIQUE (id_segement)
                );

        '''

test_meta = {

        'dim_region': {
            'file_path': 'dags/sheets/DIM_REGION.xlsx',
            'trans_table': 'trans_region',
            'upsert_query':
            '''
            INSERT INTO dim_region (id_region,  libelle_region)
            SELECT "ID_REGION", "LIBELLE_REGION" FROM trans_region
            ON CONFLICT (id_region) DO
                UPDATE SET libelle_region = EXCLUDED.libelle_region
            '''
            },

        'dim_zone': {
            'file_path': 'dags/sheets/DIM_ZONE.xlsx',
            'trans_table': 'trans_zone',
            'upsert_query':
            '''
            INSERT INTO public.dim_zone (id_zone,  libelle_zone, id_region)
            SELECT "ID_ZONE","LIBELLE_ZONE","ID_REGION " FROM public.trans_zone
            ON CONFLICT (id_zone) DO
            UPDATE SET
            libelle_zone = EXCLUDED.libelle_zone,
            id_region = EXCLUDED.id_region
            '''
            },

        'dim_client': {
            'file_path': 'dags/sheets/DIM_CLIENT.xlsx',
            'trans_table': 'trans_client',
            'upsert_query':
            '''
            INSERT INTO dim_client (id_client, nom, gsm, adresse_mail)
            SELECT "ID_CLIENT", "NOM ", "GSM", "ADRESSE_MAIL" FROM trans_client
            ON CONFLICT (id_client) DO
            UPDATE SET
                nom = EXCLUDED.nom,
                gsm = EXCLUDED.gsm,
                adresse_mail = EXCLUDED.adresse_mail
            '''
            },

        'dim_agence': {
            'file_path': 'dags/sheets/DIM_AGENCE.xlsx',
            'trans_table': 'trans_agence',
            'upsert_query':
            '''
            INSERT INTO dim_agence
            (id_agence, id_zone, id_region, libelle_agence, classe_agence)
            SELECT "ID_AGENCE", "ID_ZONE", "ID_REGION", "LIBELLE_AGENCE",
            "CLASSE_AGENCE"
            FROM trans_agence
            ON CONFLICT (id_agence)
            DO UPDATE
            SET id_zone=EXCLUDED.id_zone, id_region=EXCLUDED.id_region,
            classe_agence=EXCLUDED.classe_agence;
            '''
            },

        'dim_souscription': {
            'file_path': 'dags/sheets/DIM_SOUSCRIPTION.xlsx',
            'trans_table': 'trans_souscription',
            'upsert_query':
            '''
            INSERT INTO dim_souscription
            (id_souscrit, id_client, nom, adresse_mail, gsm, phone_verified,
             update_password, date_souscrit)
            SELECT "ID_SOUSCRIT", "ID_CLIENT", "NOM ", "ADRESSE_MAIL", "GSM",
            "PHONE_VERIFIED", "UPDATE_PASSWORD", "DATE_SOUSCRIT"
            FROM trans_souscription
            ON CONFLICT (id_souscrit)
            DO UPDATE
            SET id_client=EXCLUDED.id_client, nom=EXCLUDED.nom,
            adresse_mail=EXCLUDED.adresse_mail, gsm=EXCLUDED.gsm,
            phone_verified=EXCLUDED.phone_verified,
            update_password=EXCLUDED.update_password,
            date_souscrit=EXCLUDED.date_souscrit;

            '''
            },

        'dim_date_chequier': {
            'file_path': 'dags/sheets/DIM_DATE_CHEQUIER.xlsx',
            'trans_table': 'trans_date_chequier',
            'upsert_query':
            '''
            INSERT INTO dim_date_chequier
            (id_date_cheq, date_demande)
            SELECT "ID_DATE_CHEQ", "DATE_DEMANDE"
            FROM trans_date_chequier
            ON CONFLICT (id_date_cheq)
            DO UPDATE
            SET date_demande=EXCLUDED.date_demande;
            '''
            },

        'fact_chequier': {
            'file_path': 'dags/sheets/FACT_CHEQUIER.xlsx',
            'trans_table': 'trans_chequier',
            'upsert_query':
            '''
            INSERT INTO fact_chequier
            (id_chequier, id_client, id_date_cheq, event_category,
             event_desc, date_demande)
            SELECT "ID_CHEQUIER ", "ID_CLIENT", "ID_DATE_CHEQ",
            "EVENT_CATEGORY", "EVENT_DESC ", "DATE_DEMANDE"
            FROM trans_chequier
            ON CONFLICT (id_chequier)
            DO UPDATE
            SET id_client=EXCLUDED.id_client,
            id_date_cheq=EXCLUDED.id_date_cheq,
            event_category=EXCLUDED.event_category,
            event_desc=EXCLUDED.event_desc, date_demande=EXCLUDED.date_demande;
            '''
            },

        'fact_conquete': {
            'file_path': 'dags/sheets/FACT_CONQUETE.xlsx',
            'trans_table': 'trans_conquete',
            'upsert_query':
            '''
            INSERT INTO fact_conquete
            (id_conquete, id_client, conquete)
            SELECT "ID_CONQUETE ", "ID_CLIENT", "CONQUETE"
            FROM trans_conquete
            ON CONFLICT (id_conquete)
            DO UPDATE
            SET id_client=EXCLUDED.id_client, conquete=EXCLUDED.conquete;
            '''
            },

        'fact_date_souscription': {
            'file_path': 'dags/sheets/FACT_DATE_SOUSCRIPTION.xlsx',
            'trans_table': 'trans_date_souscription',
            'upsert_query':
            '''
            INSERT INTO fact_date_souscription
            (id_date_souscrit, id_souscrit, date_souscrit)
            SELECT "ID_DATE_SOUSCRIT", "ID_SOUSCRIT", "DATE_SOUSCRIT "
            FROM trans_date_souscription
            ON CONFLICT (id_date_souscrit)
            DO UPDATE
            SET id_souscrit=EXCLUDED.id_souscrit,
            date_souscrit=EXCLUDED.date_souscrit;
            '''
            },

        'fact_nb_client': {
            'file_path': 'dags/sheets/FACT_NB_CLIENT.xlsx',
            'trans_table': 'trans_nb_client',
            'upsert_query':
            '''
            INSERT INTO fact_nb_client
            (id_nb_client, id_agence, nb_client)
            SELECT "ID_NB_CLIENT ", "ID_AGENCE", "NB_CLIENT"
            FROM trans_nb_client
            ON CONFLICT (id_nb_client)
            DO UPDATE
            SET id_agence=EXCLUDED.id_agence, nb_client=EXCLUDED.nb_client;

            '''
            },

        'fact_nb_souscrit': {
            'file_path': 'dags/sheets/FACT_NB_SOUSCRIT.xlsx',
            'trans_table': 'trans_nb_souscrit',
            'upsert_query':
            '''
            INSERT INTO fact_nb_souscrit
            (id_nb_souscrit, id_agence, nb_souscrit)
            SELECT "ID_NB_SOUSCRIT", "ID_AGENCE", "NB_SOUSCRIT "
            FROM trans_nb_souscrit
            ON CONFLICT (id_nb_souscrit)
            DO UPDATE
            SET id_agence=EXCLUDED.id_agence, nb_souscrit=EXCLUDED.nb_souscrit;
            '''
            },

        'fact_objectifs': {
            'file_path': 'dags/sheets/FACT_OBJECTIFS.xlsx',
            'trans_table': 'trans_objectifs',
            'upsert_query':
            '''
            INSERT INTO fact_objectifs
            (id_objectif, id_agence, id_zone, id_region,
             nb_souscrit_rls, nb_souscrit_obj, date_objectif)
            SELECT "ID_OBJECTIF", "ID_AGENCE ", "ID_ZONE ", "ID_REGION ",
            "NB_SOUSCRIT_RLS", "NB_SOUSCRIT_OBJ ", "DATE_OBJECTIF "
            FROM trans_objectifs
            ON CONFLICT (id_objectif)
            DO UPDATE
            SET id_agence=EXCLUDED.id_agence, id_zone=EXCLUDED.id_zone,
            id_region=EXCLUDED.id_region,
            nb_souscrit_rls=EXCLUDED.nb_souscrit_rls,
            nb_souscrit_obj=EXCLUDED.nb_souscrit_obj;
            '''
            },

        'fact_segment': {
            'file_path': 'dags/sheets/FACT_SEGMENT.xlsx',
            'trans_table': 'trans_segment',
            'upsert_query':
            '''
            INSERT INTO fact_segement
            (id_segement, id_client, id_agence, segement)
            SELECT "ID_SEGMENT", "ID_CLIENT", "ID_AGENCE", "SEGEMENT"
            FROM trans_segment
            ON CONFLICT (id_segement)
            DO UPDATE
            SET id_client=EXCLUDED.id_client, id_agence=EXCLUDED.id_agence,
            segement=EXCLUDED.segement;
            '''
            },

        'fact_types_access': {
            'file_path': 'dags/sheets/FACT_TYPES_ACCEES.xlsx',
            'trans_table': 'trans_type_access',
            'upsert_query':
            '''
            '''
            },

        }


bundleMerge = {

       '''

       CREATE TABLE IF NOT EXISTS DIM_DATE_CHEQUIER (
                ID_DATE_CHEQ VARCHAR(255) PRIMARY KEY NOT NULL,
                ANNEES VARCHAR(255),
                MOIS INT NULL,
                JOURS INT NULL,
                HEURES TIME NULL,
                CONSTRAINT unique_date_chequier
                UNIQUE (ID_DATE_CHEQ)
                );

        CREATE TABLE IF NOT EXISTS DIM_DATE_VIREMENT (
                ID_DATE_VIRT VARCHAR(255) PRIMARY KEY NOT NULL,
                date_virement TIME,
                CONSTRAINT unique_date_virement
                UNIQUE (ID_DATE_VIRT)
                );

        CREATE TABLE IF NOT EXISTS DIM_AUTHENTIFICATION (
                ID_AUTHEN VARCHAR(255) PRIMARY KEY NOT NULL,
                ID_DATE_AUTH VARCHAR(255) NOT NULL,
                ID_SOUSCRIT VARCHAR(255),
                MOIS INT NULL,
                JOURS INT NULL,
                HEURES TIME NULL,
                CONSTRAINT unique_date_virement
                UNIQUE (ID_DATE_VIRT)
                );
       '''
}


