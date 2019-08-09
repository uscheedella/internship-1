from flask import Flask, render_template, request, flash, redirect, send_file
from werkzeug import secure_filename
from threading import Thread
import pandas as pd
import datetime
import shutil
import os
import subprocess
from influxdb import DataFrameClient
from influxdb import InfluxDBClient

app = Flask(__name__)
app.secret_key = "secret key"

#links to html file to display buttons
@app.route('/')
def upload_form():
    return render_template('romeflask.html')

#defines the submit sfile and cfile buttons
@app.route('/', methods=['POST'])
def upload_file():
    if request.method == 'POST': 
    # check if the post request has the file part
        if request.form['file_button'] == 'Submit SFile':
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
            f = request.files['file'] 
            if f == '':
                flash('No file selected for uploading')
                return redirect(request.url)
            if f:
                print(f)
                f.save(secure_filename('sfile'))
                flash('File successfully uploaded')
                return redirect('/')
            else:
                flash('Allowed file types are txt, pdf, png, jpg, jpeg, gif')
                return redirect(request.url)

        if request.form['file_button'] == 'Submit CFile':
            # check if the post request has the file part
            print("cfile")
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
            f = request.files['file']
            if f == '':
                flash('No file selected for uploading')
                return redirect(request.url)
            if f:
                print(f)
                f.save(secure_filename('cfile'))
                flash('File successfully uploaded')
                return redirect('/')
            else:
                flash('Allowed file types are txt, pdf, png, jpg, jpeg, gif')
                return redirect(request.url)

@app.route('/mergeinflux')
def mergeinflux(): 
    #opens the sfile and cfile submitted
    with open('sfile', 'r', encoding='utf8') as sfile:
        sdf = pd.read_csv(sfile, header=0, sep=';', parse_dates = ['Time'])
    with open('cfile', 'r', encoding='utf8') as cfile:
        cdf = pd.read_csv(cfile, header=0, parse_dates = ['_time'])

    #parses sdf for better reading and usage
    sdf['DR'].replace('-', '0', inplace=True)
    sdf['DR'] = pd.to_numeric(sdf['DR'])
    sdf['Successful'].replace('-', '0', inplace=True)
    sdf['Successful'] = pd.to_numeric(sdf['Successful'])
    sdf.rename(columns={"country": "Country"}, inplace=True)
    sdf['success%'].replace('-', '0', inplace=True)
    sdf['success%'] = pd.to_numeric(sdf['success%'])
    sdf.rename(columns={'success%': 'SuccessRate'}, inplace=True)
    
    #parses cdf for better reading
    cdf.rename(columns={'_time': 'Time'}, inplace=True)

    #merges sdf and cdf and creates difference column
    dfmerge = pd.merge(sdf, cdf, on=['Time','Country'])
    dfmerge['C-S'] = abs(dfmerge['CompletionRate'] - dfmerge['SuccessRate'])
   
    #saves dfmerge as a csv file in current directory
    dfmerge.to_csv(r'/home/sriya.cheedella/flaskproj/romeflask/merged', index = False, header=True)

    #saves 
    dfmerge['Time'] = pd.to_datetime(dfmerge['Time'], format='%Y-%m-%d %H:%M:%S.%f')
    dfmerge = dfmerge.set_index('Time')

    ctrycol = dfmerge['Country']
    succol = dfmerge['SuccessRate']
    compcol = dfmerge['CompletionRate']
    diffcol = dfmerge['C-S']

    statsdf = pd.DataFrame({'Country':dfmerge['Country'], 'MT':dfmerge['MT'], 'Supplier':dfmerge['Supplier'], 'supplier%':dfmerge['supplier%'], 'DR':dfmerge['DR'], 'Successful':dfmerge['Successful'], 'SuccessRate':dfmerge['SuccessRate'], 'SMSSent':dfmerge['SMSSent'], 'SMSValidated':dfmerge['SMSValidated'], 'CompletionRate':dfmerge['CompletionRate'], 'Difference':dfmerge['C-S']})
    
    clientd = DataFrameClient(host="eu1-cnws001.int.mblox.com")
    clientd.write_points(statsdf, 'totalstats', tag_columns=['Country'], protocol='json', database='rome', batch_size=10000)
    
    return send_file('/home/sriya.cheedella/flaskproj/romeflask/merged', mimetype='text/csv', attachment_filename='downloadmerged.csv', as_attachment=True)
    
if __name__ == "__main__":
    app.run(debug=False)

