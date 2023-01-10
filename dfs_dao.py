from typing import List, Tuple, Any
from psycopg2.extensions import connection, cursor

from pgConnect import PgConnection

class Dfs_dao():
    def __init__(self, pgc : PgConnection):
        self.pgc = pgc
        self.conn : connection = pgc.getConn()
        self.cur : cursor = pgc.getCurs()
    

    def draft_to_db(self, tups : List[Tuple[Any,...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s)", 
                        i).decode('utf-8') for i in tups)
        # Make and try query
        qry = "INSERT INTO draft VALUES " + (args) + " ON CONFLICT "\
                "(year, pick) DO NOTHING"
        self._try_insertion(qry, 'draft')
        
        
    def players_to_db(self, tups : List[Tuple[Any, ...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                        i).decode('utf-8') for i in tups)
        # Make and try query
        qry = "INSERT INTO player VALUES " + (args) + " ON CONFLICT "\
            "(player_name, dob, height, weight) DO NOTHING"
        self._try_insertion(qry, 'player')


    def roster_to_db(self, tups : List[Tuple[Any, ...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", 
                        i).decode('utf-8') for i in tups)
        # Make and try query
        qry = "INSERT INTO roster VALUES " + (args) + " ON CONFLICT"\
                " (season, team, player_name, dob, height, weight) DO NOTHING"
        self._try_insertion(qry, 'roster')
        

    def team_to_db(self, team_tup : Tuple[Any, ...]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s)", 
                        i).decode('utf-8') for i in team_tup)
        # Make and try query
        qry = "INSERT INTO team VALUES " + (args) + " "\
                "ON CONFLICT (season, team) DO NOTHING"
        self._try_insertion(qry, 'team')
        
    
    def _try_insertion(self, qry : str, insertion_type : str) -> None:
        try:
            self.cur.execute(qry)
            self.conn.commit()
            print(f"Committed {insertion_type} insertion!")
        except Exception as e:
            print(f"Couldn't execute and commit {insertion_type} insertion!")
            print(str(e))
            print(f"Query was... {qry}")
            