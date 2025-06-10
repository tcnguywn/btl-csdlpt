import psycopg2
import time


def getopenconnection(user='postgres', password='root', dbname='dds_assign1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def create_db(dbname):
    start_time = time.time()

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
    print(f"create_db completed in {execution_time:.4f} seconds")


def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    return count


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start_time = time.time()

    # Ensure database exists before proceeding
    create_db('dds_assign1')  # Create database if it doesn't exist

    # Get cursor from the connection
    cur = openconnection.cursor()

    try:
        # Step 1: Drop table if exists and create new one with temporary columns for parsing
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename};")

        # Build table structure with parsing columns
        table_schema = f"""
        CREATE TABLE {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER, 
            extra2 CHAR,
            rating REAL,
            extra3 CHAR,
            timestamp BIGINT
        );
        """
        cur.execute(table_schema)

        # Step 2: Use COPY FROM for fast bulk loading
        with open(ratingsfilepath, 'r') as file:
            cur.copy_from(file, ratingstablename, sep=':')

        # Step 3: Remove unnecessary columns
        cleanup_query = f"""
        ALTER TABLE {ratingstablename} 
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
        """
        cur.execute(cleanup_query)

        # Get row count for reporting
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        row_count = cur.fetchone()[0]

        # Commit the transaction
        openconnection.commit()
        print(f"Data loading successful: {row_count} records imported to {ratingstablename}")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Data loading failed: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Operation completed in {total_time:.4f} seconds")

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create range partitions for a ratings table based on the Rating value.
    """
    start_time = time.time()  
    RANGE_TABLE_PREFIX = 'range_part'
    cur = openconnection.cursor()

    try:
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i};")

        interval = 5.0 / numberofpartitions

        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating REAL
                );
            """)
            
            if i == 0:
                cur.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {ratingstablename}
                    WHERE rating >= {i * interval} AND rating <= {(i + 1) * interval};
                """)
            else:
                cur.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {ratingstablename}
                    WHERE rating > {i * interval} AND rating <= {(i + 1) * interval};
                """)

        openconnection.commit()

        # Kết thúc đo thời gian
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully created {numberofpartitions} range partitions in {elapsed_time:.4f} seconds.")

    except Exception as e:
        openconnection.rollback()
        print(f"Error in rangepartition: {str(e)}")
        raise

    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.

    Args:
        ratingstablename (str): Name of the main ratings table
        numberofpartitions (int): Number of partitions to create
        openconnection: PostgreSQL connection object
    """
    start_time = time.time()  
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    try:
        # Step 1: Create partition tables
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

        # Step 2: Distribute data using round robin approach
        # Use ROW_NUMBER() to assign sequential numbers to rows
        # Then use modulo operation to distribute to partitions
        for i in range(numberofpartitions):
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

        # Commit the transaction
        openconnection.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully created {numberofpartitions} round robin partitions in {elapsed_time:.4f} seconds.")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error creating round robin partitions: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    start_time = time.time()

    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    try:
        # Step 1: Insert into main table
        insert_main_query = f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating) 
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_main_query, (userid, itemid, rating))

        # Step 2: Get total number of rows in main table after insertion
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]

        # Step 3: Count number of existing partitions
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)

        # Step 4: Calculate which partition this new row should go to
        # Since we use 0-based indexing and round robin distribution
        partition_index = (total_rows - 1) % numberofpartitions
        partition_table_name = RROBIN_TABLE_PREFIX + str(partition_index)

        # Step 5: Insert into the appropriate partition table
        insert_partition_query = f"""
        INSERT INTO {partition_table_name} (userid, movieid, rating) 
        VALUES (%s, %s, %s);
        """
        cur.execute(insert_partition_query, (userid, itemid, rating))

        # Commit the transaction
        openconnection.commit()
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
    print(f"roundrobininsert completed in {total_time:.4f} seconds")


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    start_time = time.time()

    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = "range_part"

    if rating > 5 or rating < 0:
        print(f"Rating value is invalid: {rating}")
        return f"Rating value is invalid"

    try:
        # Step1: Insert main table
        insert_table_query = f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating) 
                VALUES (%s, %s, %s);
            """
        cur.execute(insert_table_query, (userid, itemid, rating))

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
        insert_query = (f"""
            INSERT INTO {table_name} (userid, movieid, rating) 
            VALUES (%s, %s, %s)
            """)
        cur.execute(insert_query, (userid, itemid, rating))

        openconnection.commit()
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
    print(f"rangeinsert completed in {total_time:.4f} seconds")
