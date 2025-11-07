# app.py
from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
import psycopg2.extras

app = Flask(__name__)
app.secret_key = "change-me-to-a-secret"

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="television_management",  # change if needed
        user="postgres",
        password="Nila@125",         
    )

# ---------- HOME ----------
@app.route('/')
def home():
    return render_template('home.html')

# ---------- CRUD helpers for each table ----------
# For brevity I will implement full CRUD for Channel, Genre, Show, CastAndCrew, Marketing, Advertisement, Episode, UpcomingShow.
# Each route follows a pattern: list, add (POST), update (POST), delete (GET)

# CHANNEL
@app.route('/channels')
def list_channels():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM Channel ORDER BY channel_id;")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return render_template('channels.html', channels=rows)

@app.route('/channels/add', methods=['POST'])
def add_channel():
    name = request.form['channel_name']
    satellite = request.form.get('satellite') or None
    num_of_shows = request.form.get('num_of_shows') or None
    location = request.form.get('location') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO Channel(channel_name, satellite, num_of_shows, location) VALUES (%s,%s,%s,%s)",
                (name, satellite, num_of_shows, location))
    conn.commit(); cur.close(); conn.close()
    flash('Channel added')
    return redirect(url_for('list_channels'))

@app.route('/channels/update/<int:id>', methods=['POST'])
def update_channel(id):
    name = request.form['channel_name']
    satellite = request.form.get('satellite') or None
    num_of_shows = request.form.get('num_of_shows') or None
    location = request.form.get('location') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("UPDATE Channel SET channel_name=%s, satellite=%s, num_of_shows=%s, location=%s WHERE channel_id=%s",
                (name, satellite, num_of_shows, location, id))
    conn.commit(); cur.close(); conn.close()
    flash('Channel updated')
    return redirect(url_for('list_channels'))

@app.route('/channels/delete/<int:id>')
def delete_channel(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Channel WHERE channel_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Channel deleted')
    return redirect(url_for('list_channels'))


# GENRE
@app.route('/genres')
def list_genres():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM Genre ORDER BY genre_id;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('genre.html', genres=rows)

@app.route('/genres/add', methods=['POST'])
def add_genre():
    name = request.form['genre_name']
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO Genre(genre_name) VALUES (%s)", (name,))
    conn.commit(); cur.close(); conn.close()
    flash('Genre added'); return redirect(url_for('list_genres'))

@app.route('/genres/update/<int:id>', methods=['POST'])
def update_genre(id):
    name = request.form['genre_name']
    conn = get_connection(); cur = conn.cursor()
    cur.execute("UPDATE Genre SET genre_name=%s WHERE genre_id=%s", (name,id))
    conn.commit(); cur.close(); conn.close()
    flash('Genre updated'); return redirect(url_for('list_genres'))

@app.route('/genres/delete/<int:id>')
def delete_genre(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Genre WHERE genre_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Genre deleted'); return redirect(url_for('list_genres'))


# SHOW
@app.route('/shows')
def list_shows():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""SELECT s.*, c.channel_name, g.genre_name 
                   FROM Show s
                   LEFT JOIN Channel c ON s.channel_id = c.channel_id
                   LEFT JOIN Genre g ON s.genre_id = g.genre_id
                   ORDER BY s.show_id;""")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('show.html', shows=rows)

@app.route('/shows/add', methods=['POST'])
def add_show():
    channel_id = request.form.get('channel_id') or None
    genre_id = request.form.get('genre_id') or None
    title = request.form['title']
    air_time = request.form.get('air_time') or None
    director_id = request.form['director_id']
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""INSERT INTO Show(channel_id, genre_id, title, air_time, director_id)
                   VALUES (%s,%s,%s,%s,%s)""",
                (channel_id, genre_id, title, air_time, director_id))
    conn.commit(); cur.close(); conn.close()
    flash('Show added'); return redirect(url_for('list_shows'))

@app.route('/shows/update/<int:id>', methods=['POST'])
def update_show(id):
    channel_id = request.form.get('channel_id') or None
    genre_id = request.form.get('genre_id') or None
    title = request.form['title']
    air_time = request.form.get('air_time') or None
    director_id = request.form.get('director_id') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""UPDATE Show SET channel_id=%s, genre_id=%s, title=%s, air_time=%s, director_id=%s WHERE show_id=%s""",
                (channel_id, genre_id, title, air_time, director_id, id))
    conn.commit(); cur.close(); conn.close()
    flash('Show updated'); return redirect(url_for('list_shows'))

@app.route('/shows/delete/<int:id>')
def delete_show(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Show WHERE show_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Show deleted'); return redirect(url_for('list_shows'))


# CAST AND CREW
@app.route('/castcrew')
def list_castcrew():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM CastAndCrew ORDER BY castcrew_id;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('castcrew.html', castcrew=rows)

@app.route('/castcrew/add', methods=['POST'])
def add_castcrew():
    name = request.form['name']; role_type = request.form['role_type']; show_id = request.form.get('show_id') or None; role_description = request.form.get('role_description') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO CastAndCrew(name, role_type, show_id, role_description) VALUES (%s,%s,%s,%s)",
                (name, role_type, show_id, role_description))
    conn.commit(); cur.close(); conn.close()
    flash('Person added'); return redirect(url_for('list_castcrew'))

@app.route('/castcrew/update/<int:id>', methods=['POST'])
def update_castcrew(id):
    name = request.form['name']; role_type = request.form['role_type']; show_id = request.form.get('show_id') or None; role_description = request.form.get('role_description') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("UPDATE CastAndCrew SET name=%s, role_type=%s, show_id=%s, role_description=%s WHERE castcrew_id=%s",
                (name, role_type, show_id, role_description, id))
    conn.commit(); cur.close(); conn.close()
    flash('Person updated'); return redirect(url_for('list_castcrew'))

@app.route('/castcrew/delete/<int:id>')
def delete_castcrew(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM CastAndCrew WHERE castcrew_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Person deleted'); return redirect(url_for('list_castcrew'))


# MARKETING
@app.route('/marketing')
def list_marketing():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""SELECT m.*, s.title, c.channel_name FROM Marketing m
                   LEFT JOIN Show s ON m.show_id = s.show_id
                   LEFT JOIN Channel c ON m.channel_id = c.channel_id
                   ORDER BY m.marketing_id;""")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('marketing.html', marketing=rows)

@app.route('/marketing/add', methods=['POST'])
def add_marketing():
    channel_id = request.form.get('channel_id') or None
    show_id = request.form.get('show_id') or None
    trp = request.form.get('trp') or None
    cost = request.form.get('cost_of_production') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO Marketing(channel_id, show_id, trp, cost_of_production) VALUES (%s,%s,%s,%s)",
                (channel_id, show_id, trp, cost))
    conn.commit(); cur.close(); conn.close()
    flash('Marketing added'); return redirect(url_for('list_marketing'))

@app.route('/marketing/delete/<int:id>')
def delete_marketing(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Marketing WHERE marketing_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Marketing deleted'); return redirect(url_for('list_marketing'))


# ADVERTISEMENT
@app.route('/ads')
def list_ads():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT a.*, c.channel_name FROM Advertisement a LEFT JOIN Channel c ON a.channel_id=c.channel_id ORDER BY a.ad_id;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('ads.html', ads=rows)

@app.route('/ads/add', methods=['POST'])
def add_ad():
    channel_id = request.form.get('channel_id') or None
    company = request.form.get('company_name')
    num_of_ads = request.form.get('num_of_ads') or None
    duration = request.form.get('duration') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO Advertisement(channel_id, company_name, num_of_ads, duration) VALUES (%s,%s,%s,%s)",
                (channel_id, company, num_of_ads, duration))
    conn.commit(); cur.close(); conn.close()
    flash('Ad added'); return redirect(url_for('list_ads'))

@app.route('/ads/delete/<int:id>')
def delete_ad(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Advertisement WHERE ad_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Ad deleted'); return redirect(url_for('list_ads'))


# EPISODE
@app.route('/episodes')
def list_episodes():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT e.*, s.title FROM Episode e LEFT JOIN Show s ON e.show_id = s.show_id ORDER BY e.episode_id;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('episodes.html', episodes=rows)

@app.route('/episodes/add', methods=['POST'])
def add_episode():
    show_id = request.form.get('show_id') or None
    season = request.form.get('season_number') or None
    episode_number = request.form.get('episode_number') or None
    duration = request.form.get('duration') or None
    air_date = request.form.get('air_date') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO Episode(show_id, season_number, episode_number, duration, air_date) VALUES (%s,%s,%s,%s,%s)",
                (show_id, season, episode_number, duration, air_date))
    conn.commit(); cur.close(); conn.close()
    flash('Episode added'); return redirect(url_for('list_episodes'))

@app.route('/episodes/delete/<int:id>')
def delete_episode(id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM Episode WHERE episode_id=%s",(id,))
    conn.commit(); cur.close(); conn.close()
    flash('Episode deleted'); return redirect(url_for('list_episodes'))


# UPCOMING SHOWS
@app.route('/upcoming')
def list_upcoming():
    conn = get_connection(); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT u.*, c.channel_name FROM UpcomingShow u LEFT JOIN Channel c ON u.channel_id = c.channel_id ORDER BY u.upcoming_id;")
    rows = cur.fetchall(); cur.close(); conn.close()
    return render_template('upcoming.html', upcoming=rows)

@app.route('/upcoming/add', methods=['POST'])
def add_upcoming():
    channel_id = request.form.get('channel_id') or None
    title = request.form.get('title')
    replacement = request.form.get('replacement_show_id') 
    director_id = request.form.get('director_id') or None
    conn = get_connection(); cur = conn.cursor()
    cur.execute("INSERT INTO UpcomingShow(channel_id, title, replacement_show_id, director_id) VALUES (%s,%s,%s,%s)",
                (channel_id, title, replacement, director_id))
    conn.commit(); cur.close(); conn.close()
    flash('Upcoming show added'); return redirect(url_for('list_upcoming'))

@app.route('/upcoming/promote/<int:id>')
def promote_upcoming(id):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("CALL promote_upcoming_show(%s);", (id,))
        conn.commit()
        flash('Upcoming show promoted to Show')
    except Exception as e:
        flash(f'Error: {e}')
    finally:
        cur.close(); conn.close()
    return redirect(url_for('list_upcoming'))

# ---------- Run canned complex queries (the 20) ----------
@app.route('/queries')
def queries_page():
    return render_template('queries.html')

@app.route('/run_named_query', methods=['POST'])
def run_named_query():
    name = request.form['query_name']
    conn = get_connection(); cur = conn.cursor()
    try:
        if name == 'avg_trp_per_genre':
            cur.execute("""SELECT g.genre_name, ROUND(AVG(m.trp)::numeric,2) AS avg_trp
                           FROM Genre g
                           JOIN Show s ON s.genre_id = g.genre_id
                           JOIN Marketing m ON m.show_id = s.show_id
                           GROUP BY g.genre_name;""")
        elif name == 'top3_expensive_per_channel':
            cur.execute("""SELECT channel_name, title, cost_of_production FROM (
                              SELECT c.channel_name     , s.title, m.cost_of_production,
                                     ROW_NUMBER() OVER (PARTITION BY c.channel_id ORDER BY m.cost_of_production DESC) rn
                              FROM Channel c
                              JOIN Show s ON s.channel_id = c.channel_id
                              JOIN Marketing m ON m.show_id = s.show_id
                            ) x WHERE rn <= 3;""")
        elif name == 'ad_duration_per_channel':
            cur.execute("""SELECT c.channel_name, COALESCE(SUM(a.duration), '00:00:00') AS total_ad_duration
                           FROM Channel c
                           LEFT JOIN Advertisement a ON a.channel_id = c.channel_id
                           GROUP BY c.channel_name;""")
        elif name == 'shows_trp_above9_cost_below_avg':
            cur.execute("""WITH avg_cost AS (SELECT AVG(cost_of_production) AS avgc FROM Marketing)
                           SELECT s.title, m.trp, m.cost_of_production
                           FROM Show s
                           JOIN Marketing m ON m.show_id = s.show_id
                           WHERE m.trp > 9 AND m.cost_of_production < (SELECT avgc FROM avg_cost);""")
        elif name == 'directors_no_show_below5':
            cur.execute("""SELECT d.name, d.castcrew_id
                           FROM CastAndCrew d
                           WHERE d.role_type = 'Director'
                             AND EXISTS (SELECT 1 FROM Show s WHERE s.director_id = d.castcrew_id)
                             AND NOT EXISTS (
                               SELECT 1 FROM Show s
                               JOIN Marketing m ON s.show_id = m.show_id
                               WHERE s.director_id = d.castcrew_id AND m.trp < 5
                             );""")
        elif name == 'channel_biggest_cost_diff':
            cur.execute("""SELECT channel_id, channel_name, (max_cost - min_cost) AS diff
                           FROM (
                             SELECT c.channel_id, c.channel_name,
                                    MAX(m.cost_of_production) AS max_cost,
                                    MIN(m.cost_of_production) AS min_cost
                             FROM Channel c
                             JOIN Show s ON s.channel_id = c.channel_id
                             JOIN Marketing m ON m.show_id = s.show_id
                             GROUP BY c.channel_id, c.channel_name
                           ) t
                           ORDER BY diff DESC
                           LIMIT 1;""")
        elif name == 'channel_summary_view':
            cur.execute("SELECT * FROM channel_summary;")
        elif name == 'generate_weekly_schedule':
            # call procedure as example for channel 1 and date today
            cur.execute("CALL generate_weekly_schedule(%s, %s);", (1, '2025-10-27'))
            conn.commit()
            cur.execute("SELECT * FROM ShowSchedule WHERE channel_id = 1 ORDER BY scheduled_date, scheduled_time;")
        elif name == 'shows_short_high_trp':
            cur.execute("""SELECT sd.title, sd.total_duration, m.trp
                           FROM show_details sd
                           JOIN Marketing m ON sd.show_id = m.show_id
                           WHERE sd.total_duration < INTERVAL '00:40:00' AND m.trp > 8.5;""")
        elif name == 'trp_above_avg':
            cur.execute("""WITH avg_trp AS (SELECT AVG(trp) as avg_trp FROM Marketing)
                           SELECT s.title, m.trp FROM Show s JOIN Marketing m ON s.show_id = m.show_id
                           WHERE m.trp > (SELECT avg_trp FROM avg_trp);""")
        elif name == 'cost_per_genre_channel':
            cur.execute("""SELECT c.channel_name, g.genre_name, SUM(m.cost_of_production) AS total_cost
                           FROM Marketing m
                           JOIN Show s ON m.show_id = s.show_id
                           JOIN Channel c ON c.channel_id = s.channel_id
                           JOIN Genre g ON g.genre_id = s.genre_id
                           GROUP BY c.channel_name, g.genre_name
                           ORDER BY c.channel_name, total_cost DESC;""")
        elif name == 'directors_multiple_genres':
            cur.execute("""SELECT d.castcrew_id, d.name, COUNT(DISTINCT s.genre_id) AS num_genres
                           FROM CastAndCrew d
                           JOIN Show s ON s.director_id = d.castcrew_id
                           GROUP BY d.castcrew_id, d.name
                           HAVING COUNT(DISTINCT s.genre_id) > 1;""")
        elif name == 'directors_also_actors_shows':
            cur.execute("""WITH duals AS (
                             SELECT name, castcrew_id FROM CastAndCrew
                             WHERE name IN (SELECT name FROM CastAndCrew WHERE role_type = 'Actor')
                               AND name IN (SELECT name FROM CastAndCrew WHERE role_type = 'Director')
                           )
                           SELECT s.title, d.name AS director
                           FROM Show s
                           JOIN CastAndCrew d ON s.director_id = d.castcrew_id
                           WHERE d.castcrew_id IN (SELECT castcrew_id FROM duals);""")
        elif name == 'avg_episode_duration_per_season':
            cur.execute("""SELECT show_id, season_number, AVG(duration) as avg_duration
                           FROM Episode
                           GROUP BY show_id, season_number
                           ORDER BY show_id, season_number;""")
        elif name == 'upcoming_replacing_low_trp':
            cur.execute("""SELECT u.upcoming_id, u.title, s.title AS replaced_title, m.trp
                           FROM UpcomingShow u
                           LEFT JOIN Show s ON u.replacement_show_id = s.show_id
                           LEFT JOIN Marketing m ON s.show_id = m.show_id
                           WHERE m.trp < 9 OR m.trp IS NULL;""")
        elif name == 'prevent_trp_trigger_test':
            # Attempt to insert a bad TRP to show trigger works (will raise). Insert handled in app form normally.
            cur.execute("INSERT INTO Marketing(channel_id, show_id, trp, cost_of_production) VALUES (1,1,11,1000);")
            conn.commit()
            cur.execute("SELECT 'should not reach here';")
        elif name == 'add_show_with_validation':
            # example call
            cur.execute("CALL add_show_valid(%s,%s,%s,%s,%s);", (1, None, 'New show from app', None, None))
            conn.commit()
            cur.execute("SELECT 'Added (check shows)';")
        elif name == 'top_3_shows':
          cur.execute("""
            SELECT s.title, c.channel_name, m.trp
            FROM Show s
            JOIN Channel c ON s.channel_id = c.channel_id
            JOIN Marketing m ON s.show_id = m.show_id
            ORDER BY m.trp DESC
            LIMIT 3;
        """)
          conn.commit()
            
        elif name == 'avg_trp_per_channel':
          cur.execute("""
            SELECT c.channel_name, ROUND(AVG(m.trp), 2) AS avg_trp
            FROM Channel c
            JOIN Show s ON c.channel_id = s.channel_id
            JOIN Marketing m ON s.show_id = m.show_id
            GROUP BY c.channel_name;
        """)
        

        elif name == 'prime_time_shows':
          cur.execute("""
            SELECT s.title, c.channel_name, s.air_time
            FROM Show s
            JOIN Channel c ON s.channel_id = c.channel_id
            WHERE s.air_time BETWEEN '19:00:00' AND '22:00:00'
            ORDER BY s.air_time;
        """)
        
        elif name == 'top_director':
          cur.execute("""
           SELECT c.name, ROUND(AVG(m.trp),2) AS avg_trp
FROM CastAndCrew c
JOIN Show s ON c.castcrew_id = s.director_id
JOIN Marketing m ON s.show_id = m.show_id
GROUP BY c.name
ORDER BY avg_trp DESC
LIMIT 1;

        """)

        elif name == 'top_expensive_channel':
          cur.execute("""
            SELECT c.channel_name, SUM(m.cost_of_production) AS total
            FROM Channel c
            JOIN Show s ON c.channel_id = s.channel_id
            JOIN Marketing m ON s.show_id = m.show_id
            GROUP BY c.channel_name
            ORDER BY total DESC
            LIMIT 1;
        """)
        


        else:
            cur.execute("SELECT 'Unknown query';")
        rows = cur.fetchall()
    except Exception as e:
        conn.rollback()
        rows = [(f'Error: {e}',)]
    finally:
        cur.close(); conn.close()
    return render_template('query_result.html', rows=rows, name=name)

if __name__ == '__main__':
    app.run(debug=True)
