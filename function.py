import psycopg2
import time


def getopenconnection(user='postgres', password='root', dbname='db_assign1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def create_db(dbname):
    start_time = time.time()
    print(f"[TIMING] Starting create_db for '{dbname}'")

    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"[TIMING] create_db completed in {execution_time:.4f} seconds")


def count_partitions(prefix, openconnection):
    start_time = time.time()

    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    end_time = time.time()
    execution_time = end_time - start_time
    print(
        f"[TIMING] count_partitions for '{prefix}' completed in {execution_time:.4f} seconds - Found {count} partitions")

    return count


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start_time = time.time()
    print(f"[TIMING] Starting loadratings - Table: {ratingstablename}, File: {ratingsfilepath}")

    # Ensure database exists before proceeding
    create_db('db_assign1')  # Create database if it doesn't exist

    # Get cursor from the connection
    cur = openconnection.cursor()

    try:
        # Step 1: Create the ratings table with correct schema
        table_start_time = time.time()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            movieid INTEGER, 
            rating REAL
        );
        """
        cur.execute(create_table_query)
        table_end_time = time.time()
        print(f"[TIMING] Table creation took {table_end_time - table_start_time:.4f} seconds")

        # Step 2: Clear existing data if any
        clear_start_time = time.time()
        cur.execute(f"DELETE FROM {ratingstablename};")
        clear_end_time = time.time()
        print(f"[TIMING] Table clearing took {clear_end_time - clear_start_time:.4f} seconds")

        # Step 3: Read and parse the data file
        insert_start_time = time.time()
        row_count = 0
        with open(ratingsfilepath, 'r') as file:
            for line in file:
                # Parse each line: UserID::MovieID::Rating::Timestamp
                parts = line.strip().split('::')
                if len(parts) >= 4:  # Ensure we have at least 4 parts
                    userid = int(parts[0])
                    movieid = int(parts[1])
                    rating = float(parts[2])
                    # We ignore timestamp (parts[3]) as per schema requirement

                    # Insert the record
                    insert_query = f"""
                    INSERT INTO {ratingstablename} (userid, movieid, rating) 
                    VALUES (%s, %s, %s);
                    """
                    cur.execute(insert_query, (userid, movieid, rating))
                    row_count += 1

                    # Progress indicator
                    if row_count % 10000 == 0:
                        current_time = time.time()
                        elapsed = current_time - insert_start_time
                        print(f"[PROGRESS] Loaded {row_count} rows in {elapsed:.2f} seconds")

        insert_end_time = time.time()
        insert_time = insert_end_time - insert_start_time
        print(f"[TIMING] Data insertion took {insert_time:.4f} seconds for {row_count} rows")

        # Commit the transaction
        commit_start_time = time.time()
        openconnection.commit()
        commit_end_time = time.time()
        print(f"[TIMING] Commit took {commit_end_time - commit_start_time:.4f} seconds")

        print(f"Successfully loaded data into {ratingstablename}")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error loading data: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TIMING] loadratings TOTAL TIME: {total_time:.4f} seconds")


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    start_time = time.time()
    print(f"[TIMING] Starting rangepartition - {numberofpartitions} partitions for {ratingstablename}")

    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = "range_part"
    # range value in each horizontal fragment
    delta = 5 / numberofpartitions

    try:
        for i in range(numberofpartitions):
            partition_start_time = time.time()

            # Step1: Create partition tables
            minRange = i * delta
            maxRange = minRange + delta
            table_name = RANGE_TABLE_PREFIX + str(i)

            create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            userid INTEGER,
                            movieid INTEGER,
                            rating REAL
                        );
                        """
            cur.execute(create_table_query)

            # Clear existing data if any
            cur.execute(f"DELETE FROM {table_name};")

            # Step 2: Insert data from ratingtables to each range_part table
            if i == 0:
                insert_data_query = f"""
                        INSERT INTO {table_name} (userid, movieid, rating) 
                            SELECT userid, movieid, rating 
                            FROM {ratingstablename} 
                            WHERE rating >= {str(minRange)} AND rating <= {str(maxRange)};
                """
            else:
                insert_data_query = f"""
                        INSERT INTO {table_name} (userid, movieid, rating) 
                            SELECT userid, movieid, rating 
                            FROM {ratingstablename} 
                            WHERE rating > {str(minRange)} AND rating <= {str(maxRange)};
                """
            cur.execute(insert_data_query)

            # Get count of inserted rows
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            partition_count = cur.fetchone()[0]

            partition_end_time = time.time()
            partition_time = partition_end_time - partition_start_time
            print(
                f"[TIMING] Partition {table_name} [{minRange:.2f}-{maxRange:.2f}]: {partition_count} rows in {partition_time:.4f} seconds")

        # Commit Transaction
        commit_start_time = time.time()
        openconnection.commit()
        commit_end_time = time.time()
        print(f"[TIMING] Commit took {commit_end_time - commit_start_time:.4f} seconds")

        print(f"Successfully create {numberofpartitions} range partitions")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error creating range partitions: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TIMING] rangepartition TOTAL TIME: {total_time:.4f} seconds")


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    start_time = time.time()
    print(f"[TIMING] Starting roundrobinpartition - {numberofpartitions} partitions for {ratingstablename}")

    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    try:
        # Step 1: Create partition tables
        table_creation_start = time.time()
        for i in range(numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating REAL
            );
            """
            cur.execute(create_table_query)

            # Clear existing data if any
            cur.execute(f"DELETE FROM {table_name};")

        table_creation_end = time.time()
        print(
            f"[TIMING] Created {numberofpartitions} partition tables in {table_creation_end - table_creation_start:.4f} seconds")

        # Step 2: Distribute data using round robin approach
        # Use ROW_NUMBER() to assign sequential numbers to rows
        # Then use modulo operation to distribute to partitions
        for i in range(numberofpartitions):
            partition_start_time = time.time()
            table_name = RROBIN_TABLE_PREFIX + str(i)

            insert_query = f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating 
            FROM (
                SELECT userid, movieid, rating, 
                       ROW_NUMBER() OVER() as row_num
                FROM {ratingstablename}
            ) as numbered_rows
            WHERE (row_num - 1) % {numberofpartitions} = {i};
            """
            cur.execute(insert_query)

            # Get count of inserted rows
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            partition_count = cur.fetchone()[0]

            partition_end_time = time.time()
            partition_time = partition_end_time - partition_start_time
            print(f"[TIMING] Partition {table_name}: {partition_count} rows in {partition_time:.4f} seconds")

        # Commit the transaction
        commit_start_time = time.time()
        openconnection.commit()
        commit_end_time = time.time()
        print(f"[TIMING] Commit took {commit_end_time - commit_start_time:.4f} seconds")

        print(f"Successfully created {numberofpartitions} round robin partitions")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error creating round robin partitions: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TIMING] roundrobinpartition TOTAL TIME: {total_time:.4f} seconds")


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    start_time = time.time()
    print(f"[TIMING] Starting roundrobininsert - userid: {userid}, itemid: {itemid}, rating: {rating}")

    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    try:
        # Step 1: Insert into main table
        main_insert_start = time.time()
        insert_main_query = f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating) 
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_main_query, (userid, itemid, rating))
        main_insert_end = time.time()
        print(f"[TIMING] Main table insert took {main_insert_end - main_insert_start:.4f} seconds")

        # Step 2: Get total number of rows in main table after insertion
        count_start = time.time()
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]
        count_end = time.time()
        print(f"[TIMING] Row count query took {count_end - count_start:.4f} seconds - Total rows: {total_rows}")

        # Step 3: Count number of existing partitions
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)

        # Step 4: Calculate which partition this new row should go to
        # Since we use 0-based indexing and round robin distribution
        partition_index = (total_rows - 1) % numberofpartitions
        partition_table_name = RROBIN_TABLE_PREFIX + str(partition_index)

        # Step 5: Insert into the appropriate partition table
        partition_insert_start = time.time()
        insert_partition_query = f"""
        INSERT INTO {partition_table_name} (userid, movieid, rating) 
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_partition_query, (userid, itemid, rating))
        partition_insert_end = time.time()
        print(
            f"[TIMING] Partition {partition_table_name} insert took {partition_insert_end - partition_insert_start:.4f} seconds")

        # Commit the transaction
        commit_start = time.time()
        openconnection.commit()
        commit_end = time.time()
        print(f"[TIMING] Commit took {commit_end - commit_start:.4f} seconds")

        print(f"Successfully inserted record into {ratingstablename} and {partition_table_name}")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error inserting record: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TIMING] roundrobininsert TOTAL TIME: {total_time:.4f} seconds")


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    start_time = time.time()
    print(f"[TIMING] Starting rangeinsert - userid: {userid}, itemid: {itemid}, rating: {rating}")

    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = "range_part"

    if rating > 5 or rating < 0:
        print(f"[ERROR] Invalid rating value: {rating}")
        return f"Rating value is invalid"

    try:
        # Step1: Insert main table
        main_insert_start = time.time()
        insert_table_query = f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating) 
                VALUES (%s, %s, %s);
            """
        cur.execute(insert_table_query, (userid, itemid, rating))
        main_insert_end = time.time()
        print(f"[TIMING] Main table insert took {main_insert_end - main_insert_start:.4f} seconds")

        # Step 2: Count numbers of partitions
        numbers = count_partitions(RANGE_TABLE_PREFIX, openconnection)

        # Step 3: Calculate range value of each partition
        delta = 5 / numbers  # max rating = 5

        # Step 4: Define partition that will insert
        index = int(rating / delta)
        if rating % delta == 0 and index != 0:
            index -= 1

        table_name = RANGE_TABLE_PREFIX + str(index)

        # Step 5: Insert data into partition
        partition_insert_start = time.time()
        insert_query = (f"""
            INSERT INTO {table_name} (userid, movieid, rating) 
            VALUES (%s, %s, %s)
            """)
        cur.execute(insert_query, (userid, itemid, rating))
        partition_insert_end = time.time()
        print(
            f"[TIMING] Partition {table_name} insert took {partition_insert_end - partition_insert_start:.4f} seconds")

        commit_start = time.time()
        openconnection.commit()
        commit_end = time.time()
        print(f"[TIMING] Commit took {commit_end - commit_start:.4f} seconds")

        print(f"Successfully inserted record into {ratingstablename} and {table_name}")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error inserting record: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TIMING] rangeinsert TOTAL TIME: {total_time:.4f} seconds")