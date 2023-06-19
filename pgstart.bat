
@REM https://gist.github.com/jctosta/baa4a1ba472a5999f445c0f43fdbe208

@REM initdb.exe -D ..\data --username=postgres --auth=trust

@REM set PATH=%PATH%;C:\3Projects\rapidiam\venv\Scripts\
@REM set PATH=%PATH%;C:\3Projects\rapidiam\venv\


@REM C:\Users\pranjan24\Downloads\postgresql-14.2-2-windows-x64-binaries\pgsql\bin\pg_ctl -D C:\Users\pranjan24\Downloads\postgresql-14.2-2-windows-x64-binaries\pgsql\data -l logfile stop
@REM C:\Users\pranjan24\Downloads\postgresql-14.2-2-windows-x64-binaries\pgsql\bin\pg_ctl -D C:\Users\pranjan24\Downloads\postgresql-14.2-2-windows-x64-binaries\pgsql\data -l logfile start

@REM SET PYTHONPATH C:\3Projects\rapidiam\venv\Scripts
@REM SET PATH=%PATH%;PYTHONPATH
SET CLIENT_ENCODING TO 'utf8';

C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\bin\pg_ctl -D C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\data -l logfile stop

C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\bin\pg_ctl -D C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\data -l logfile start

C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\bin\pg_dump -U postgres postgres > C:\3Projects\rapidiam\backup\backup.sql

@REM C:\Users\pranjan24\Downloads\postgresql-14.6-1-windows-x64-binaries\pgsql\bin\psql -U postgres -d postgres < C:\3Projects\rapidiam\backup\backup.sql
@REM C:\Users\pranjan24\AppData\Local\Programs\Python\Python310\Scripts\pip3.10
@REM npx postgraphile -c postgres://postgres:postgres@localhost:5432/postgres?ssl=false --watch
@REM postgres://postgres:postgres@localhost:5432/data?ssl=false

@REM npx postgraphile -c 'postgres://postgres:postgres@localhost:5432/postgres'  --watch --enhance-graphiql --dynamic-json --schema public
