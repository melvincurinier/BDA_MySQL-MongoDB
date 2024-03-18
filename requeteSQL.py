def requete1(con):
    cur = con.cursor()
    res = cur.execute('''
                      SELECT m.primaryTitle FROM movies m 
                      JOIN characters c ON m.mid = c.mid 
                      JOIN persons p ON c.pid = p.pid 
                      WHERE p.primaryName = 'Jean Reno'
                      ''')
    return res

def requete2(con):  
    cur = con.cursor()
    res = cur.execute('''
                      SELECT m.primaryTitle, r.averageRating
                      FROM movies m
                      JOIN ratings r ON m.mid = r.mid 
                      JOIN genres g ON m.mid = g.mid
                      WHERE g.genre = 'Horror' 
                      AND m.startYear BETWEEN 2000 and 2009
                      ORDER BY r.averageRating DESC
                      LIMIT 3;
    ''')
    return res

def requete3(con):
    cur = con.cursor()
    res = cur.execute('''
                      SELECT p.*
                        FROM persons p
                        WHERE p.pid IN (
                            SELECT w.pid
                            FROM writers w
                            WHERE w.mid NOT IN (
                                SELECT t.mid
                                FROM titles t
                                WHERE t.region = 'ES'
                            )
                        );
                      ''')
    return res

def requete4(con): 
    cur = con.cursor()
    res = cur.execute('''
                    SELECT actor_name, movie_title, role_count
                    FROM (
                        SELECT p.primaryName AS actor_name, m.primaryTitle AS movie_title, COUNT(*) AS role_count 
                        FROM persons p 
                        JOIN characters c ON p.pid = c.pid 
                        JOIN movies m ON c.mid = m.mid 
                        GROUP BY c.pid, c.mid 
                    ) AS roles_per_actor_movie
                    WHERE role_count = (
                        SELECT MAX(role_count) 
                        FROM (
                            SELECT COUNT(*) AS role_count 
                            FROM persons p 
                            JOIN characters c ON p.pid = c.pid 
                            JOIN movies m ON c.mid = m.mid 
                            GROUP BY c.pid, c.mid 
                        ) AS max_roles
                    )
                    ORDER BY role_count DESC;
                    ''')
    return res

def requete5(con):
    cur = con.cursor()
    res = cur.execute('''
                      SELECT DISTINCT persons.primaryName FROM persons
                      INNER JOIN knownformovies ON persons.pid = knownformovies.pid
                      INNER JOIN movies ON knownformovies.mid = movies.mid
                      INNER JOIN ratings ON movies.mid = ratings.mid
                      WHERE movies.startYear < (SELECT startYear FROM movies WHERE primaryTitle = 'Avatar') 
                      AND ratings.numVotes < 200000
                      AND movies.mid NOT IN (
                        SELECT movies.mid
                        FROM movies
                        INNER JOIN ratings ON movies.mid = ratings.mid
                        WHERE movies.startYear >= (SELECT startYear FROM movies WHERE primaryTitle = 'Avatar') 
                      AND ratings.numVotes > 200000)
                     '''
    )
    return res