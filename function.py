import psycopg2

def getopenconnection(user='postgres', password='123456', dbname='postgres'):
    """
    Create and return a PostgreSQL connection
    """
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
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


def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    return count


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data from ratingsfilepath file to a table called ratingstablename.

    Args:
        ratingstablename (str): Name of the table to create and load data into
        ratingsfilepath (str): Absolute path to the ratings.dat file
        openconnection: PostgreSQL connection object
    """

    # Ensure database exists before proceeding
    create_db('db_assign')  # Create database if it doesn't exist

    # Get cursor from the connection
    cur = openconnection.cursor()

    try:
        # Step 1: Create the ratings table with correct schema
        # Schema: UserID (int), MovieID (int), Rating (float)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            movieid INTEGER, 
            rating REAL
        );
        """
        cur.execute(create_table_query)

        # Step 2: Clear existing data if any
        cur.execute(f"DELETE FROM {ratingstablename};")

        # Step 3: Read and parse the data file
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

        # Commit the transaction
        openconnection.commit()
        print(f"Successfully loaded data into {ratingstablename}")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error loading data: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()


# Test function to demonstrate usage
def test_loadratings():
    """
    Test function to show how to use loadratings
    """
    try:
        # Create connection
        conn = getopenconnection()

        # Load ratings
        loadratings('ratings', 'test_data.dat', conn)

        # Verify data loaded
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ratings;")
        count = cur.fetchone()[0]
        print(f"Total records loaded: {count}")

        # Show sample data
        cur.execute("SELECT * FROM ratings LIMIT 5;")
        rows = cur.fetchall()
        print("Sample data:")
        for row in rows:
            print(f"UserID: {row[0]}, MovieID: {row[1]}, Rating: {row[2]}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Test failed: {str(e)}")

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
        Function to create partitions of main table using round robin approach.

        Args:
            ratingstablename (str): Name of the main ratings table
            numberofpartitions (int): Number of partitions to create
            openconnection: PostgreSQL connection object
        """

    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = "range_part"
    # range value in each horizontal fragment
    delta = 5 / numberofpartitions
    try:

        for i in range(numberofpartitions):
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

            #Step 2: Insert data from ratingtables to each range_part table
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

        #Commit Transaction
        openconnection.commit()
        print(f"Successfully create {numberofpartitions} range partitions")
    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error creating range partitions: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.

    Args:
        ratingstablename (str): Name of the main ratings table
        numberofpartitions (int): Number of partitions to create
        openconnection: PostgreSQL connection object
    """

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
        print(f"Successfully created {numberofpartitions} round robin partitions")

    except Exception as e:
        # Rollback in case of error
        openconnection.rollback()
        print(f"Error creating round robin partitions: {str(e)}")
        raise e

    finally:
        # Close cursor (but not connection as per requirement)
        cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin approach.

    Args:
        ratingstablename (str): Name of the main ratings table
        userid (int): User ID of the new record
        itemid (int): Movie ID of the new record
        rating (float): Rating value of the new record
        openconnection: PostgreSQL connection object
    """

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

        # Step 5: Insert into the appropriate partition table
        partition_table_name = RROBIN_TABLE_PREFIX + str(partition_index)
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

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
        Function to insert a new row into the main table and specific partition based on round robin approach.

        Args:
            ratingstablename (str): Name of the main ratings table
            userid (int): User ID of the new record
            itemid (int): Movie ID of the new record
            rating (float): Rating value of the new record
            openconnection: PostgreSQL connection object
    """

    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = "range_part"

    if rating > 5 or rating < 0:
        return f"Rating value is invalid"

    try:
        # Step1: Insert main table
        insert_table_query = f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating) 
                VALUES (%s, %s, %s);
            """
        cur.execute(insert_table_query, (userid, itemid, rating))

        #Step 2: Count numbers of partitions
        numbers = count_partitions(RANGE_TABLE_PREFIX, openconnection)

        #Step 3: Calculate range value of each partition
        delta = 5 / numbers # max rating = 5

        #Step 4: Define partition that will insert
        index = int(rating/delta)
        if rating % delta == 0 and index != 0:
            index -=1

        # Step 5: Insert data into partition
        table_name = RANGE_TABLE_PREFIX + str(index)
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
