from flask import Flask, render_template, jsonify, request
import sqlite3

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/jobs')
def get_jobs():
    conn = sqlite3.connect('calcareers_jobs.db')
    cursor = conn.cursor()

    cursor.execute('SELECT id, Classification, Working_Title, Department, Final_Filing_Date, Salary, Location, Job_Control_Number, job_URL, Job_Description, Desirable_Qualifications, Special_Requirements, Status FROM jobs')
    jobs = cursor.fetchall()

    conn.close()

    job_list = []
    for job in jobs:
        job_list.append({
            "id": job[0],
            "Classification": job[1],
            "Working Title": job[2],
            "Department": job[3],
            "Final Filing Date": job[4],
            "Salary": job[5],
            "Location": job[6],
            "Job Control Number": job[7],
            "Job URL": job[8],
            "Job Description": job[9],
            "Desirable Qualifications": job[10],
            "Special Requirements": job[11],
            "Status": job[12]
        })

    return jsonify(job_list)

@app.route('/update_status', methods=['POST'])
def update_status():
    data = request.json
    job_id = data['id']
    status = data['status']

    conn = sqlite3.connect('calcareers_jobs.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE jobs SET Status = ? WHERE id = ?', (status, job_id))
    conn.commit()
    conn.close()

    return jsonify({"success": True})

if __name__ == '__main__':
    app.run(debug=True)