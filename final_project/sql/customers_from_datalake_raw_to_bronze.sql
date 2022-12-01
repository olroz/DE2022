
MERGE `{{ params.project_id }}.bronze.customers` DWH
USING (
    SELECT
        Id,
        FirstName,
        LastName,
        Email,
        RegistrationDate,
        State
    FROM customers_csv
    GROUP BY Id,FirstName,LastName,Email,RegistrationDate,State
) DL
ON DWH.Id = DL.Id
WHEN NOT MATCHED THEN
    INSERT(Id, FirstName, LastName, Email, RegistrationDate, State, _logical_dt, _job_start_dt)
    VALUES(Id, FirstName, LastName, Email, RegistrationDate, State, CAST('{{ dag_run.logical_date }}' AS TIMESTAMP), CAST('{{ dag_run.start_date }}'AS TIMESTAMP))
WHEN MATCHED THEN
    UPDATE SET
        FirstName = DL.FirstName,
        LastName = DL.LastName,
        Email = DL.Email,
        RegistrationDate = DL.RegistrationDate,
        State = DL.State,
        _job_start_dt = CAST('{{ dag_run.start_date }}'AS TIMESTAMP)
;
