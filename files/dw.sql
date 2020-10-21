#Build data warehouse via ETL process
#Table m
Drop Table if exists m;
Create Table m as 
Select a.Member_Number as Member_ID, Membership_Type,  (2019-Year_Joined) as Years, Number_of_People, holder_First_Name,holder_Last_Name,holder_Gender,holder_BirthDate
From (Select Member_Number, max(Member_suffix) as Number_of_People From members 
Group By Member_Number) as a 
Join (Select Member_Number, First_Name as holder_First_Name,Last_Name as 
holder_Last_Name,Gender as holder_Gender,BirthDate as holder_Birthdate from members 
Where Relationship_to_Member ="Member") as b
Using(Member_Number)
Join memberships as ms
Using(Member_Number);


# Table consum
Drop Table if exists consum;
Create Table consum as
Select Member_Number, sum(Brunch) as Brunch, sum(Lunch) as Lunch, sum(Dinner_Weekday) as Dinner_Weekday, sum(Dinner_Weekend) as Dinner_Weekend, sum(Special) as Special,sum(Dining) as Dining, sum(Pool) as Pool, sum(Golf) as Golf, sum(Tennis) as Tennis,
sum(Other) as Other
From
(Select Member_Number, if(Service='Brunch_Weekend',Total,0) as Brunch, if (Service='Lunch',Total,0) as Lunch, if (Service='Dinner_Weekday',Total,0) as Dinner_Weekday,
if (Service='Dinner_Weekend',Total,0) as Dinner_Weekend, if (Service='Special',Total,0) as Special ,
Total as Dining, 0 as Golf, 0 as Pool, 0 as Tennis, 0 as  Other From dining
Union All
Select Member_Number, 0 as Brunch, 0 as Lunch, 0 as Dinner_Weekday, 0 as Dinner_Weekend, 0 as Special, 0 as Dining, Amount as Golf, 0 as Pool, 0 as Tennis, 0 as Other From Golf
Union All
Select Member_Number, 0 as Brunch, 0 as Lunch, 0 as Dinner_Weekday, 0 as Dinner_Weekend, 0 as Special,  0 as Dining, 0 as Golf, Amount as Pool, 0 as Tennis, 0 as Other From Pool
Union All
Select Member_Number, 0 as Brunch, 0 as Lunch, 0 as Dinner_Weekday, 0 as Dinner_Weekend, 0 as Special, 0 as Dining, 0 as Golf, 0 as Pool, Amount as Tennis, 0 as Other From Tennis
Union All
Select Member_Number, 0 as Brunch, 0 as Lunch, 0 as Dinner_Weekday, 0 as Dinner_Weekend, 0 as Special, 0 as Dining, 0 as Golf, 0 as Pool, 0 as Tennis, Amount as Other
From Other) as c
Group By Member_Number;



# Aggregation
Drop Table if exists dw;
Create Table dw as
Select m.*, Coalesce(Brunch) as Brunch, Coalesce(Lunch) as Lunch, Coalesce(Dinner_Weekday) as Dinner_Weekday, Coalesce(Dinner_Weekend) as Dinner_Weekend, Coalesce(Special) as Special,Coalesce(Dining) as Dining,Coalesce(Pool) as Pool, Coalesce(Golf) as Golf, Coalesce(Tennis) as Tennis,
Coalesce(Other) as Other,  (Dining+Golf+Pool+Tennis+Other) as Total,
round((Dining+Golf+Pool+Tennis+Other)/
Number_of_People,2) as Total_Per_Person, if(promoone.Member_Number  is NULL, 0, 1) as PromoOne,
if(promotwo.Member_Number  is NULL, 0, 1) as PromoTwo,
Coalesce(`Private Function`,0) as Private_Function,
Coalesce(`4th of July`,0) as July_Fourth,
Coalesce(Thanksgiving,0) as Thanksgiving,
Coalesce(`Easter Brunch`,0) as Easter_Brunch
From m
Left Join consum
On m.Member_ID= consum.Member_Number
Left Join promoone 
On m.Member_ID= promoone.Member_Number
Left Join promotwo 
On m.Member_ID= promotwo.Member_Number
Left Join special 
On m.Member_ID= special.Member_Number
Order By Member_ID;

#View data warehouse dw
Select * From dw;




#Ad-hoc Queries
#Query 1
Select 
sum(Dining),sum(Golf),sum(Pool),sum(Tennis),sum(Other)
From dw;

#Query 2
Select 
sum(Brunch),sum(Lunch),sum(Dinner_Weekday),sum(Dinner_Weekend),sum(Special)
From dw;

#Query 3
Select Membership_Type,avg(Total),avg(Total_Per_Person)
From dw
Group By Membership_Type;

#Query 4
Select 
Membership_Type,sum(Dining),sum(Golf),sum(Pool),sum(Tennis),sum(Other)
From dw
Group By Membership_Type;

# Query 5
Select
    Membership_Type, count(*),
    avg(Private_Function) as Private_Function_Rate, avg(July_Fourth) as July_Fourth_Rate, avg(Thanksgiving) as Tanksgiving_Rate, avg(Easter_Brunch) as Easter_Brunch_Rate
From
    dw
Group By Membership_Type
Order By Count(*) DESC; 

#Query 6
Select 
Membership_Type,promoone,sum(Dining),sum(Golf),sum(Pool),sum(Tennis),sum(Other)
From dw
Group By Membership_Type,promoone;

Select 
Membership_Type,promotwo,sum(Dining),sum(Golf),sum(Pool),sum(Tennis),sum(Other)
From dw
Group By Membership_Type,promotwo;






