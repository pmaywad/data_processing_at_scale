#!/usr/bin/python2.7
# 
# Interface for the assignement
#

import psycopg2

ROUND_ROBIN_PREFIX = "rrobin_part"
RANGE_PART_PREFIX = "range_part"


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    conn = openconnection
    cur = conn.cursor()

    #Creating Table and loading data to the table
    cur.execute("DROP TABLE IF EXISTS %s;" % ratingstablename)
    cur.execute("CREATE TABLE %s (\
                userid INTEGER,\
                temp1 CHAR,\
                movieid INTEGER, \
                temp2 CHAR,\
                rating NUMERIC(2,1) CHECK (rating <= 5 AND rating >= 0),\
                temp3 CHAR,\
                timestamp BIGINT, \
                PRIMARY KEY (userid,movieid));" % ratingstablename)

    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')

    cur.execute("ALTER TABLE %s DROP COLUMN temp1, \
                DROP COLUMN temp2, \
                DROP COLUMN temp3, \
                DROP COLUMN timestamp;" % ratingstablename)

    conn.commit()
    cur.close()

    

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    """
    An algorithm to partition the ratings table based on N uniform ranges of the Rating attribute.
    """
    conn = openconnection
    cur = conn.cursor()
    common_diff = 5.0/numberofpartitions
    for i in range(numberofpartitions):
        min_rating = i*common_diff
        max_rating = min_rating + common_diff
        table_name = RANGE_PART_PREFIX + str(i)
        #cur.execute("DROP TABLE IF EXISTS %s ;" % table_name)
        if i == 0:
            cmd = "CREATE TABLE %s AS SELECT * FROM %s WHERE rating >= %s AND rating<= %s;" %(table_name, ratingstablename, str(min_rating), str(max_rating))
            cur.execute(cmd)
        else:
            cmd = "CREATE TABLE %s AS SELECT * FROM %s WHERE rating > %s AND rating<= %s;" %(table_name, ratingstablename, str(min_rating), str(max_rating))
            cur.execute(cmd)

    conn.commit()
    cur.close()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    """
    An algorithm to generates N horizontal fragments of the Ratings table using the round robin partitioning approach.
    """

    cur = openconnection.cursor()
    for i in range(numberofpartitions):
        table_name = ROUND_ROBIN_PREFIX + str(i)
        #cur.execute("DROP TABLE IF EXISTS %s ;" % table_name)
        cmd = "CREATE TABLE %s AS SELECT userid, movieid, rating FROM (SELECT *, row_number() over() FROM %s) as temp \
                     WHERE (row_number-1)%%(%s) = %s;" %(table_name, ratingstablename, str(numberofpartitions), str(i))
        cur.execute(cmd)

    openconnection.commit()
    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES ( %s, %s, %s);"\
                 %(ratingstablename, str(userid), str(itemid), str(rating)))

    cur.execute("SELECT * FROM %s;" %ratingstablename)
    rows = cur.fetchall()[0][0]
    numberofpartitions = countpartition(ROUND_ROBIN_PREFIX, openconnection)
    idx = rows-1%numberofpartitions
    cur.execute("INSERT INTO %s%s (userid, movieid, rating) VALUES (%s, %s, %s);" \
                %(ROUND_ROBIN_PREFIX, str(idx), str(userid), str(itemid), str(rating)))
    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES ( %s, %s, %s);"\
                 %(ratingstablename, str(userid), str(itemid), str(rating)))
    
    numberofpartitions = countpartition(RANGE_PART_PREFIX, openconnection)
    common_diff = 5.0/numberofpartitions
    idx = int(rating//common_diff)
    if rating%common_diff == 0 and idx != 0:
        idx = idx - 1

    cmd = "INSERT INTO %s%s (userid, movieid, rating) VALUES (%s, %s, %s);" \
                %(RANGE_PART_PREFIX, str(idx), str(userid), str(itemid), str(rating))

    cur.execute(cmd)
    openconnection.commit()
    cur.close()



def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def countpartition(table_prefix, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(table_prefix))
    count = int(cur.fetchone()[0])
    cur.close()
    return count


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
