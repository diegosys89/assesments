select d.department,
j.job,
sum(case 
    when FORMAT_DATE("%Q(%Y)", cast(substr(datetime,0,10) as date)) = "1(2021)"
    then 1 else 0 end) as Q1,
sum(case 
    when FORMAT_DATE("%Q(%Y)", cast(substr(datetime,0,10) as date)) = "2(2021)"
    then 1 else 0 end) as Q2,
sum(case 
    when FORMAT_DATE("%Q(%Y)", cast(substr(datetime,0,10) as date)) = "3(2021)"
    then 1 else 0 end) as Q3,
sum(case 
    when FORMAT_DATE("%Q(%Y)", cast(substr(datetime,0,10) as date)) = "4(2021))"
    then 1 else 0 end) as Q4,
FROM `makikuna-integral.globant_assessment.hired_employees` e
left join `makikuna-integral.globant_assessment.departments` d on e.department_id = d.id
left join `makikuna-integral.globant_assessment.jobs` j on e.job_id = j.id
group by 1,2