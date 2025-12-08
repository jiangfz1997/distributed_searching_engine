import sqlite3;


c=sqlite3.connect('./data/output/inverted.db');
row=c.execute("SELECT * FROM inverted_index WHERE term='march'").fetchone();
print(f'Term: {row[0]}\nDoc Count: {len(row[1].split(','))}\nPostings: {row[1][:100]}...') if row else print('Not Found')
print(f'Total Terms: {c.execute('SELECT count(*) FROM inverted_index').fetchone()[0]}')