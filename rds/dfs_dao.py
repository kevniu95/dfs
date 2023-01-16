from typing import List, Tuple, Any, Dict
from psycopg2.extensions import connection, cursor

from pgConnect import PgConnection

class Dfs_dao():
    def __init__(self, pgc : PgConnection):
        self.pgc = pgc
        self.conn : connection = pgc.getConn()
        self.cur : cursor = pgc.getCurs()
    
    
    def get_team_game_num(self, date1 : str, date2 : str) -> Dict[str, int]:
        qry = """ SELECT tm1, COUNT(*)
                    FROM team_box
                    WHERE (game_date >= '""" + date1 + \
                    """' AND game_date <= '""" + date2 + """')
                    GROUP BY tm1;"""
        res : List[Tuple[Any,...]] = self._try_select(qry, 'team_game_num')
        return dict(res)


    def validate_no_tm_date_dups(self, date1 : str, date2 : str) -> None:
        qry = """SELECT tm1, game_date, COUNT(*) as total_rows
                    FROM team_box
                    WHERE (game_date >='"""  + (date1) + """' AND game_date <= '""" + (date2) + \
                    """')
                    GROUP BY tm1, game_date
                    HAVING COUNT(*) > 1
                    ORDER BY total_rows DESC;"""
        
        res : List[Tuple[Any, ...]]= self._try_select(qry, 'no team date dups')
        assert len(res) == 0
        
        
    def team_box_to_db(self, tups : List[Tuple[Any, ...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                            i).decode('utf-8') for i in tups)
        
        qry = "INSERT INTO team_box VALUES " + (args) + " ON CONFLICT "\
                "(game_date, game_time, tm1, tm2) DO NOTHING"
        self._try_insertion(qry, 'team_box')


    def player_box_to_db(self, tups : List[Tuple[Any,...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                                            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"\
                                            ")",
                                            i).decode('utf-8') for i in tups)
        qry = "INSERT INTO player_box VALUES " + (args) + " ON CONFLICT "\
                "(game_date, game_time, tm1, tm2, player_name) DO NOTHING"
        self._try_insertion(qry, 'player_box')


    def draft_to_db(self, tups : List[Tuple[Any,...]]) -> None:
        args = ','.join(self.cur.mogrify("(%s,%s,%s,%s,%s,%s)", 
                        i).decode('utf-8') for i in tups)
        
        # Make and try query
        qry = "INSERT INTO draft VALUES " + (args) + " ON CONFLICT "\
                "(year, pick) DO NOTHING"
        print(qry)
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
        
    
    def _try_select(self, qry : str, select_type : str) -> List[Tuple[Any,...]]:
        try: 
            self.cur.execute(qry)
            res : List[Tuple[Any,...]] = self.cur.fetchall()
            return res
        except Exception as e:
            print(f"Couldn't execute and commit {select_type} select!")
            print(str(e))
            print(f"Query was... {qry}")        


    def _try_insertion(self, qry : str, insertion_type : str) -> None:
        try:
            self.cur.execute(qry)
            self.conn.commit()
            print(f"Committed {insertion_type} insertion!")
        except Exception as e:
            print(f"Couldn't execute and commit {insertion_type} insertion!")
            print(str(e))
            print(f"Query was... {qry}")        