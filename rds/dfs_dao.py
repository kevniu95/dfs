import argparse
from typing import List, Tuple, Any, Dict
from psycopg2.extensions import connection, cursor

from config import Config
from pgConnect import PgConnection

class Dfs_dao():
    def __init__(self, pgc : PgConnection):
        self.pgc = pgc
        self.conn : connection = pgc.getConn()
        self.cur : cursor = pgc.getCurs()
    
    """
    A. Utility functions
    """
    def delete_entries_in_date_range(self, table : str, date1: str, date2 : str) -> None:
        sql = """DELETE FROM """ + (table) + \
                    """ WHERE game_date >= %s and game_date <=%s;"""
        data = (date1, date2)
        self._try_commit(sql, data, 'delete')
        
    """
    Queries
    """
    def select_game_date_times(self) -> List[Tuple[Any, Any, Any]]:
        sql = """SELECT DISTINCT game_date, MIN(game_time), MAX(game_time)
                FROM team_box
                GROUP BY game_date
                ORDER BY game_date DESC;"""
        res = self._try_select(sql, (), 'select_times')
        return res

    """
    B. Validation functions
    """
    # ======
    # 1. Game Numbers
    # ======
    def get_team_game_num(self, date1 : str, date2 : str) -> Dict[str, int]:
        data = (date1, date2)
        sql = """ SELECT tm1, COUNT(*)
                    FROM team_box
                    WHERE (game_date >= %s AND game_date <= %s)
                    GROUP BY tm1;"""
        res : List[Tuple[Any,...]] = self._try_select(sql, data, 'team_game_num')
        return dict(res)


    def get_team_game_num_fp(self, date1 : str, date2 : str) -> Dict[str, int]:
        data = (date1, date2)
        sql = """SELECT tm1, COUNT(*)
                FROM
                (SELECT DISTINCT game_date, tm1
                FROM player_box) as A
                WHERE game_date >= %s and game_date <= %s
                GROUP BY tm1;"""
        res : List[Tuple[Any,...]] = self._try_select(sql, data, 'team_game_num_from_player')
        return dict(res)

    # =======
    # 2. Validation checks
    # =======
    def validate_same_box_games(self) -> bool:
        data=()
        sql = """ SELECT a.*
                    FROM player_box as a
                    FULL JOIN
                    (SELECT DISTINCT game_date, tm1, tm2
                    FROM team_box) AS b
                    ON a.game_date = b.game_date AND a.tm1 = b.tm1 AND a.tm2 = b.tm2
                    WHERE b.game_date IS NULL;"""
        res : List[Tuple[Any,...]] = self._try_select(sql, data, 'team_game_num_from_player')
        assert len(res) == 0
        return True

    def validate_internal_box_score_consistency(self) -> bool:
        data = ()
        games = """SELECT DISTINCT game_date,tm1, tm2
                    FROM team_box;"""

        # 1. Games are consistent in score
        print("-Checking that games internally consistent in score...")
        gc = """SELECT * 
                    FROM 
                    (SELECT game_date, tm1, tm1 as tm1_, tm2, tm2 as tm2_, pts, pts as pts_, pts_a, pts_a as pts_a_
                    FROM team_box) AS a
                    JOIN 
                    (SELECT game_date, tm1, tm1 as tm1_, tm2, tm2 as tm2_, pts, pts as pts_, pts_a, pts_a as pts_a_
                    FROM team_box) AS b
                    ON a.tm1 = b.tm2_ 
                        AND a.tm2 = b.tm1_
                        AND a.game_date = b.game_date
                        AND a.pts = b.pts_a_
                        AND a.pts_a = b.pts_;"""
        
        # 2. Game scores consistent with player totals
        print("-Checking that games scores are consistent with player points...")
        gc_w_p = """ SELECT a.game_date, a.tm1, a.tm2, a.player_pts, b.pts
                        FROM (SELECT DISTINCT game_date, tm1, tm2, SUM(pts) as player_pts 
                        FROM player_box
                        GROUP BY game_date, tm1, tm2) AS a
                        FULL JOIN team_box as b
                        ON a.game_date = b.game_date
                        AND a.tm1 = b.tm1
                        AND a.tm2 = b.tm2
                        AND a.player_pts = b.pts;"""
        a = self._try_select(games, data, 'game scores')
        b = self._try_select(gc, data, 'game_scores')
        c = self._try_select(gc_w_p, data, 'game_scores')
        assert len(a) == len(c) == len(b)
        return True
	

    def validate_no_tm_date_dups(self, date1 : str, date2 : str) -> bool:
        data = (date1, date2)
        sql = """SELECT tm1, game_date, COUNT(*) as total_rows
                    FROM team_box
                    WHERE (game_date >= %s AND game_date <= %s)
                    GROUP BY tm1, game_date
                    HAVING COUNT(*) > 1
                    ORDER BY total_rows DESC;"""
        res : List[Tuple[Any, ...]]= self._try_select(sql, data, 'no team date dups')
        assert len(res) == 0
        return True
        

    """
    C. Upload to DB
    """
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

    """
    D. Helpers
    """
    def _try_commit(self, sql : str, data : Tuple[str,...], commit_type : str) -> None:
        try:
            self.cur.execute(sql, data)
            self.conn.commit()
            print(f"Committed {commit_type}!")
        except Exception as e:
            print(f"Couldn't execute and commit {commit_type} insertion!")
            print(str(e))
            print(f"Query was... {sql} with data... {data}")  


    def _try_select(self, sql : str, data : Tuple[str,...], select_type : str) -> List[Tuple[Any,...]]:
        try: 
            self.cur.execute(sql, data)
            res : List[Tuple[Any,...]] = self.cur.fetchall()
            return res
        except Exception as e:
            print(f"Couldn't execute and commit {select_type} select!")
            print(str(e))
            print(f"Query was... {sql} with data ... {data}")        


    def _try_insertion(self, qry : str, insertion_type : str) -> None:
        print("Trying insertion now...")
        print("Here is the cursor")
        print(self.cur)
        try:
            self.cur.execute(qry)
            self.conn.commit()
            print(f"Committed {insertion_type} insertion!")
        except Exception as e:
            print(f"Couldn't execute and commit {insertion_type} insertion!")
            print(str(e))
            print(f"Query was... {qry}")        
      

if __name__ == '__main__':
    # ======
    # 1. Read configs
    # ======
    config : Config = Config()
    pgc : PgConnection = PgConnection(config)
    dao : Dfs_dao = Dfs_dao(pgc)

    # ======
    # 2. Parse arguments
    # ======
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', action = argparse.BooleanOptionalAction)
    parser.add_argument("--table", help = "Table to be processed", nargs = '?')
    parser.add_argument("--date1", help = "Start date to be processed", nargs = '?')
    parser.add_argument("--date2", help = "End date to be processed", nargs = '?')

    args = parser.parse_args()

    if args.delete:
        dao.delete_entries_in_date_range(args.table, args.date1, args.date2)
    
    dao.select_game_date_times()
    