with 
hired_by_dep as 
(
  select department_id, sum(1) as hires
  FROM `makikuna-integral.globant_assessment.hired_employees`
  group by 1
),
avg_2021 as 
(
  select avg(hires) as avg2021 from hired_by_dep
),
hires as (
  select department_id, avg(hires) as hired from hired_by_dep
  group by 1 having avg(hires) > (select avg2021 from avg_2021)
)
select h.department_id, d.department, h.hired from hires h
left join `makikuna-integral.globant_assessment.departments` d on h.department_id = d.id
