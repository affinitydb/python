#!/usr/bin/env python2.6
# Copyright (c) 2004-2014 GoPivotal, Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# -----
"""This module is a another basic test for notifications, more focused on
quantitative assessment (than testnotifications1)."""

from testfwk import AffinityTest
from affinity import *
from afynotifier import *
import random
import time

def _entryPoint():
    lAffinity = AFFINITY()
    lAffinity.open(pKeepAlive=True)
    #AFYNOTIFIER.open(lAffinity)

    print ("1. create a few classes")
    lAffinity.setPrefix("tn2c", "http://localhost/afy/class/testnotifications2/")
    lAffinity.setPrefix("tn2p", "http://localhost/afy/property/testnotifications2/")
    try:
        lAffinity.q("CREATE CLASS tn2c:Person AS SELECT * WHERE tn2p:name IN :0;")
        lAffinity.q("CREATE CLASS tn2c:Location AS SELECT * WHERE BEGINS(tn2p:postalcode, :0);")
        lAffinity.q("CREATE CLASS tn2c:Occupation AS SELECT * WHERE BEGINS(tn2p:occupation, :0);")
        lAffinity.q("CREATE CLASS tn2c:Age AS SELECT * WHERE BEGINS(tn2p:age, :0);")
    except:
        pass

    print ("2. create lots of pins of each")
    lFirstNames = [ \
        'Aaron', 'Abby', 'Abigail', 'Ada', 'Adam', 'Addie', 'Adela', 'Adele', 'Adeline', 'Adrian', 'Adriana', 'Adrienne', 'Agnes', 'Aida', 'Aileen', 'Aimee', 'Aisha', 'Alan', 'Alana', 'Alba', 'Albert', 'Alberta', 'Alberto', 'Alejandra', 'Alex', 'Alexander', 'Alexandra', 'Alexandria', 'Alexis', 'Alfred', 'Alfreda', 'Alfredo', 'Alice', 'Alicia', 'Aline', 'Alisa', 'Alisha', 'Alison', 'Alissa', 'Allan', 'Allen', 'Allie', 'Allison', 'Allyson', 'Alma', 'Alta', 'Althea', 'Alvin', 'Alyce', 'Alyson', 'Alyssa', 'Amalia', 'Amanda', 'Amber', 'Amelia', 'Amie', 'Amparo', 'Amy', 'Ana', 'Anastasia', 'Andre', 'Andrea', 'Andrew', 'Andy', 'Angel', 'Angela', 'Angelia', 'Angelica', 'Angelina', 'Angeline', 'Angelique', 'Angelita', 'Angie', 'Anita', 'Ann', 'Anna', 'Annabelle', 'Anne', 'Annette', 'Annie', 'Annmarie', 'Anthony', 'Antoinette', 'Antonia', 'Antonio', 'April', 'Araceli', 'Arlene', 'Arline', 'Armando', 'Arnold', 'Arthur', 'Ashlee', 'Ashley', 'Audra', 'Audrey', 'Augusta', 'Aurelia', 'Aurora', 'Autumn', 'Ava', 'Avis', 'Barbara', 'Barbra', 'Barry', 'Beatrice', 'Beatriz', 'Becky', 'Belinda', 'Ben', 'Benita', 'Benjamin', 'Bernadette', 'Bernadine', 'Bernard', 'Bernice', 'Berta', 'Bertha', 'Bertie', 'Beryl', 'Bessie', 'Beth', 'Bethany', 'Betsy', 'Bette', 'Bettie', 'Betty', 'Bettye', 'Beulah', 'Beverley', 'Beverly', 'Bianca', 'Bill', 'Billie', 'Billy', 'Blanca', 'Blanche', 'Bob', 'Bobbi', 'Bobbie', 'Bobby', 'Bonita', 'Bonnie', 'Brad', 'Bradley', 'Brandi', 'Brandie', 'Brandon', 'Brandy', 'Brenda', 'Brent', 'Brett', 'Brian', 'Briana', 'Brianna', 'Bridget', 'Bridgett', 'Bridgette', \
        'Brigitte', 'Britney', 'Brittany', 'Brittney', 'Brooke', 'Bruce', 'Bryan', 'Byron', 'Caitlin', 'Callie', 'Calvin', 'Camille', 'Candace', 'Candice', 'Candy', 'Cara', 'Carey', 'Carissa', 'Carl', 'Carla', 'Carlene', 'Carlos', 'Carly', 'Carmela', 'Carmella', 'Carmen', 'Carol', 'Carole', 'Carolina', 'Caroline', 'Carolyn', 'Carrie', 'Casandra', 'Casey', 'Cassandra', 'Cassie', 'Catalina', 'Catherine', 'Cathleen', 'Cathryn', 'Cathy', 'Cecelia', 'Cecil', 'Cecile', 'Cecilia', 'Celeste', 'Celia', 'Celina', 'Chad', 'Chandra', 'Charity', 'Charlene', 'Charles', 'Charlie', 'Charlotte', 'Charmaine', 'Chasity', 'Chelsea', 'Cheri', 'Cherie', 'Cherry', 'Cheryl', 'Chester', 'Chris', 'Christa', 'Christi', 'Christian', 'Christie', 'Christina', 'Christine', 'Christopher', 'Christy', 'Chrystal', 'Cindy', 'Claire', 'Clara', 'Clare', 'Clarence', 'Clarice', 'Clarissa', 'Claude', 'Claudette', 'Claudia', 'Claudine', 'Clayton', 'Cleo', 'Clifford', 'Clifton', 'Clinton', 'Clyde', 'Cody', 'Coleen', 'Colette', 'Colleen', 'Concepcion', 'Concetta', 'Connie', 'Constance', 'Consuelo', 'Cora', 'Corey', 'Corina', 'Corine', 'Corinne', 'Cornelia', 'Corrine', 'Cory', 'Courtney', 'Craig', 'Cristina', 'Crystal', 'Curtis', 'Cynthia', 'Daisy', 'Dale', 'Dan', 'Dana', 'Daniel', 'Danielle', 'Danny', 'Daphne', 'Darcy', 'Darla', 'Darlene', 'Darrell', 'Darren', 'Darryl', 'Daryl', 'Dave', 'David', 'Dawn', 'Dean', 'Deana', 'Deann', 'Deanna', 'Deanne', 'Debbie', 'Debora', 'Deborah', 'Debra', 'Dee', 'Deena', 'Deidre', 'Deirdre', 'Delia', 'Della', 'Delores', 'Deloris', 'Dena', 'Denise', 'Dennis', 'Derek', 'Derrick', \
        'Desiree', 'Diana', 'Diane', 'Diann', 'Dianna', 'Dianne', 'Dina', 'Dionne', 'Dixie', 'Dollie', 'Dolly', 'Dolores', 'Dominique', 'Don', 'Dona', 'Donald', 'Donna', 'Dora', 'Doreen', 'Doris', 'Dorothea', 'Dorothy', 'Dorthy', 'Douglas', 'Duane', 'Dustin', 'Dwayne', 'Dwight', 'Earl', 'Earlene', 'Earline', 'Earnestine', 'Ebony', 'Eddie', 'Edgar', 'Edith', 'Edna', 'Eduardo', 'Edward', 'Edwin', 'Edwina', 'Effie', 'Eileen', 'Elaine', 'Elba', 'Eleanor', 'Elena', 'Elinor', 'Elisa', 'Elisabeth', 'Elise', 'Eliza', 'Elizabeth', 'Ella', 'Ellen', 'Elma', 'Elmer', 'Elnora', 'Eloise', 'Elsa', 'Elsie', 'Elva', 'Elvia', 'Elvira', 'Emilia', 'Emily', 'Emma', 'Enid', 'Eric', 'Erica', 'Ericka', 'Erik', 'Erika', 'Erin', 'Erma', 'Erna', 'Ernest', 'Ernestine', 'Esmeralda', 'Esperanza', 'Essie', 'Estela', 'Estella', 'Estelle', 'Ester', 'Esther', 'Ethel', 'Etta', 'Eugene', 'Eugenia', 'Eula', 'Eunice', 'Eva', 'Evangelina', 'Evangeline', 'Eve', 'Evelyn', 'Everett', 'Faith', 'Fannie', 'Fanny', 'Fay', 'Faye', 'Felecia', 'Felicia', 'Felix', 'Fern', 'Fernando', 'Flora', 'Florence', 'Florine', 'Flossie', 'Floyd', 'Fran', 'Frances', 'Francesca', 'Francine', 'Francis', 'Francisca', 'Francisco', 'Frank', 'Frankie', 'Franklin', 'Fred', 'Freda', 'Frederick', 'Freida', 'Frieda', 'Gabriel', 'Gabriela', 'Gabrielle', 'Gail', 'Gale', 'Gary', 'Gay', 'Gayle', 'Gena', 'Gene', 'Geneva', 'Genevieve', 'George', 'Georgette', 'Georgia', 'Georgina', 'Gerald', 'Geraldine', 'Gertrude', 'Gilbert', 'Gilda', 'Gina', 'Ginger', 'Gladys', 'Glen', 'Glenda', 'Glenn', 'Glenna', 'Gloria', 'Goldie', 'Gordon', 'Grace', 'Gracie', \
        'Graciela', 'Greg', 'Gregory', 'Greta', 'Gretchen', 'Guadalupe', 'Guy', 'Gwen', 'Gwendolyn', 'Haley', 'Hallie', 'Hannah', 'Harold', 'Harriet', 'Harriett', 'Harry', 'Harvey', 'Hattie', 'Hazel', 'Heather', 'Hector', 'Heidi', 'Helen', 'Helena', 'Helene', 'Helga', 'Henrietta', 'Henry', 'Herbert', 'Herman', 'Herminia', 'Hester', 'Hilary', 'Hilda', 'Hillary', 'Hollie', 'Holly', 'Hope', 'Howard', 'Hugh', 'Ian', 'Ida', 'Ila', 'Ilene', 'Imelda', 'Imogene', 'Ina', 'Ines', 'Inez', 'Ingrid', 'Irene', 'Iris', 'Irma', 'Isaac', 'Isabel', 'Isabella', 'Isabelle', 'Iva', 'Ivan', 'Ivy', 'Jack', 'Jackie', 'Jacklyn', 'Jaclyn', 'Jacob', 'Jacqueline', 'Jacquelyn', 'Jaime', 'James', 'Jami', 'Jamie', 'Jan', 'Jana', 'Jane', 'Janell', 'Janelle', 'Janet', 'Janette', 'Janice', 'Janie', 'Janine', 'Janis', 'Janna', 'Jannie', 'Jared', 'Jasmine', 'Jason', 'Javier', 'Jay', 'Jayne', 'Jean', 'Jeanette', 'Jeanie', 'Jeanine', 'Jeanne', 'Jeannette', 'Jeannie', 'Jeannine', 'Jeff', 'Jeffery', 'Jeffrey', 'Jenifer', 'Jenna', 'Jennie', 'Jennifer', 'Jenny', 'Jeremy', 'Jeri', 'Jerome', 'Jerri', 'Jerry', 'Jesse', 'Jessica', 'Jessie', 'Jesus', 'Jewel', 'Jewell', 'Jill', 'Jillian', 'Jim', 'Jimmie', 'Jimmy', 'Jo', 'Joan', 'Joann', 'Joanna', 'Joanne', 'Jocelyn', 'Jodi', 'Jodie', 'Jody', 'Joe', 'Joel', 'Johanna', 'John', 'Johnnie', 'Johnny', 'Jolene', 'Jon', 'Jonathan', 'Joni', 'Jordan', 'Jorge', 'Jose', 'Josefa', 'Josefina', 'Joseph', 'Josephine', 'Joshua', 'Josie', 'Joy', 'Joyce', 'Juan', 'Juana', 'Juanita', 'Judith', 'Judy', 'Julia', 'Julian', 'Juliana', 'Julianne', 'Julie', 'Juliet', 'Juliette', 'Julio', 'June', \
        'Justin', 'Justine', 'Kaitlin', 'Kara', 'Karen', 'Kari', 'Karin', 'Karina', 'Karl', 'Karla', 'Karyn', 'Kasey', 'Kate', 'Katelyn', 'Katharine', 'Katherine', 'Katheryn', 'Kathie', 'Kathleen', 'Kathrine', 'Kathryn', 'Kathy', 'Katie', 'Katina', 'Katrina', 'Katy', 'Kay', 'Kaye', 'Kayla', 'Keisha', 'Keith', 'Kelley', 'Kelli', 'Kellie', 'Kelly', 'Kelsey', 'Ken', 'Kendra', 'Kenneth', 'Kent', 'Kenya', 'Keri', 'Kerri', 'Kerry', 'Kevin', 'Kim', 'Kimberley', 'Kimberly', 'Kirk', 'Kirsten', 'Kitty', 'Kris', 'Krista', 'Kristen', 'Kristi', 'Kristie', 'Kristin', 'Kristina', 'Kristine', 'Kristy', 'Krystal', 'Kurt', 'Kyle', 'Lacey', 'Lacy', 'Ladonna', 'Lakeisha', 'Lakisha', 'Lana', 'Lance', 'Lara', 'Larry', 'Latasha', 'Latisha', 'Latonya', 'Latoya', 'Laura', 'Laurel', 'Lauren', 'Lauri', 'Laurie', 'Laverne', 'Lavonne', 'Lawanda', 'Lawrence', 'Lea', 'Leah', 'Leann', 'Leanna', 'Leanne', 'Lee', 'Leigh', 'Leila', 'Lela', 'Lelia', 'Lena', 'Lenora', 'Lenore', 'Leo', 'Leola', 'Leon', 'Leona', 'Leonard', 'Leonor', 'Leroy', 'Lesa', 'Lesley', 'Leslie', 'Lessie', 'Lester', 'Leta', 'Letha', 'Leticia', 'Letitia', 'Lewis', 'Lidia', 'Lila', 'Lilia', 'Lilian', 'Liliana', 'Lillian', 'Lillie', 'Lilly', 'Lily', 'Lina', 'Linda', 'Lindsay', 'Lindsey', 'Lisa', 'Liz', 'Liza', 'Lizzie', 'Lloyd', 'Lois', 'Lola', 'Lolita', 'Lonnie', 'Lora', 'Loraine', 'Lorena', 'Lorene', 'Loretta', 'Lori', 'Lorie', 'Lorna', 'Lorraine', 'Lorrie', 'Lottie', 'Lou', 'Louella', 'Louis', 'Louisa', 'Louise', 'Lourdes', 'Luann', 'Lucia', 'Lucile', 'Lucille', 'Lucinda', 'Lucy', 'Luella', 'Luis', 'Luisa', 'Lula', 'Lupe', 'Luz', 'Lydia', \
        'Lynda', 'Lynette', 'Lynn', 'Lynne', 'Lynnette', 'Mabel', 'Mable', 'Madeleine', 'Madeline', 'Madelyn', 'Madge', 'Mae', 'Magdalena', 'Maggie', 'Mai', 'Malinda', 'Mallory', 'Mamie', 'Mandy', 'Manuel', 'Manuela', 'Mara', 'Marc', 'Marcella', 'Marci', 'Marcia', 'Marcie', 'Marcus', 'Marcy', 'Margaret', 'Margarita', 'Margery', 'Margie', 'Margo', 'Margret', 'Marguerite', 'Mari', 'Maria', 'Marian', 'Mariana', 'Marianne', 'Maribel', 'Maricela', 'Marie', 'Marietta', 'Marilyn', 'Marina', 'Mario', 'Marion', 'Marisa', 'Marisol', 'Marissa', 'Maritza', 'Marjorie', 'Mark', 'Marla', 'Marlene', 'Marquita', 'Marsha', 'Marshall', 'Marta', 'Martha', 'Martin', 'Martina', 'Marva', 'Marvin', 'Mary', 'Maryann', 'Maryanne', 'Maryellen', 'Marylou', 'Mathew', 'Matilda', 'Matthew', 'Mattie', 'Maude', 'Maura', 'Maureen', 'Maurice', 'Mavis', 'Max', 'Maxine', 'May', 'Mayra', 'Meagan', 'Megan', 'Meghan', 'Melanie', 'Melba', 'Melinda', 'Melisa', 'Melissa', 'Melody', 'Melva', 'Melvin', 'Mercedes', 'Meredith', 'Merle', 'Mia', 'Michael', 'Micheal', 'Michele', 'Michelle', 'Miguel', 'Mike', 'Milagros', 'Mildred', 'Millicent', 'Millie', 'Milton', 'Mindy', 'Minerva', 'Minnie', 'Miranda', 'Miriam', 'Misty', 'Mitchell', 'Mitzi', 'Mollie', 'Molly', 'Mona', 'Monica', 'Monique', 'Morgan', 'Morris', 'Muriel', 'Myra', 'Myrna', 'Myrtle', 'Nadia', 'Nadine', 'Nancy', 'Nanette', 'Nannie', 'Naomi', 'Natalia', 'Natalie', 'Natasha', 'Nathan', 'Nathaniel', 'Neil', 'Nelda', 'Nell', 'Nellie', 'Nelson', 'Nettie', 'Neva', 'Nicholas', 'Nichole', 'Nicole', 'Nikki', 'Nina', 'Nita', 'Noelle', 'Noemi', 'Nola', 'Nona', 'Nora', 'Noreen', \
        'Norma', 'Norman', 'Odessa', 'Ofelia', 'Ola', 'Olga', 'Olive', 'Olivia', 'Ollie', 'Opal', 'Ophelia', 'Ora', 'Oscar', 'Paige', 'Pam', 'Pamela', 'Pansy', 'Pat', 'Patrica', 'Patrice', 'Patricia', 'Patrick', 'Patsy', 'Patti', 'Patty', 'Paul', 'Paula', 'Paulette', 'Pauline', 'Pearl', 'Pearlie', 'Pedro', 'Peggy', 'Penelope', 'Penny', 'Perry', 'Peter', 'Petra', 'Philip', 'Phillip', 'Phoebe', 'Phyllis', 'Polly', 'Priscilla', 'Queen', 'Rachael', 'Rachel', 'Rachelle', 'Rae', 'Rafael', 'Ralph', 'Ramon', 'Ramona', 'Randall', 'Randi', 'Randy', 'Raquel', 'Raul', 'Ray', 'Raymond', 'Reba', 'Rebecca', 'Rebekah', 'Regina', 'Reginald', 'Rena', 'Rene', 'Renee', 'Reva', 'Reyna', 'Rhea', 'Rhoda', 'Rhonda', 'Ricardo', 'Richard', 'Rick', 'Ricky', 'Rita', 'Robbie', 'Robert', 'Roberta', 'Roberto', 'Robin', 'Robyn', 'Rochelle', 'Rodney', 'Roger', 'Roland', 'Ron', 'Ronald', 'Ronda', 'Ronnie', 'Rosa', 'Rosalie', 'Rosalind', 'Rosalinda', 'Rosalyn', 'Rosanna', 'Rosanne', 'Rosario', 'Rose', 'Roseann', 'Rosella', 'Rosemarie', 'Rosemary', 'Rosetta', 'Rosie', 'Roslyn', 'Ross', 'Rowena', 'Roxanne', 'Roxie', 'Roy', 'Ruben', 'Ruby', 'Russell', 'Ruth', 'Ruthie', 'Ryan', 'Sabrina', 'Sadie', 'Sallie', 'Sally', 'Salvador', 'Sam', 'Samantha', 'Samuel', 'Sandra', 'Sandy', 'Sara', 'Sarah', 'Sasha', 'Saundra', 'Savannah', 'Scott', 'Sean', 'Selena', 'Selma', 'Serena', 'Sergio', 'Seth', 'Shana', 'Shane', 'Shanna', 'Shannon', 'Shari', 'Sharlene', 'Sharon', 'Sharron', 'Shauna', 'Shawn', 'Shawna', 'Sheena', 'Sheila', 'Shelby', 'Shelia', 'Shelley', 'Shelly', 'Sheree', 'Sheri', 'Sherri', 'Sherrie', 'Sherry', 'Sheryl', 'Shirley', \
        'Sidney', 'Silvia', 'Simone', 'Socorro', 'Sofia', 'Sondra', 'Sonia', 'Sonja', 'Sonya', 'Sophia', 'Sophie', 'Stacey', 'Staci', 'Stacie', 'Stacy', 'Stanley', 'Stefanie', 'Stella', 'Stephanie', 'Stephen', 'Steve', 'Steven', 'Sue', 'Summer', 'Susan', 'Susana', 'Susanna', 'Susanne', 'Susie', 'Suzanne', 'Suzette', 'Sybil', 'Sylvia', 'Tabatha', 'Tabitha', 'Tamara', 'Tameka', 'Tamera', 'Tami', 'Tamika', 'Tammi', 'Tammie', 'Tammy', 'Tamra', 'Tania', 'Tanisha', 'Tanya', 'Tara', 'Tasha', 'Taylor', 'Ted', 'Teresa', 'Teri', 'Terra', 'Terrance', 'Terrence', 'Terri', 'Terrie', 'Terry', 'Tessa', 'Thelma', 'Theodore', 'Theresa', 'Therese', 'Thomas', 'Tia', 'Tiffany', 'Tim', 'Timothy', 'Tina', 'Tisha', 'Todd', 'Tom', 'Tommie', 'Tommy', 'Toni', 'Tonia', 'Tony', 'Tonya', 'Tracey', 'Traci', 'Tracie', 'Tracy', 'Travis', 'Tricia', 'Trina', 'Trisha', 'Troy', 'Trudy', 'Twila', 'Tyler', 'Tyrone', 'Ursula', 'Valarie', 'Valeria', 'Valerie', 'Vanessa', 'Velma', 'Vera', 'Verna', 'Vernon', 'Veronica', 'Vicki', 'Vickie', 'Vicky', 'Victor', 'Victoria', 'Vilma', 'Vincent', 'Viola', 'Violet', 'Virgie', 'Virgil', 'Virginia', 'Vivian', 'Vonda', 'Wallace', 'Walter', 'Wanda', 'Warren', 'Wayne', 'Wendi', 'Wendy', 'Wesley', 'Whitney', 'Wilda', 'Willa', 'Willard', 'William', 'Willie', 'Wilma', 'Winifred', 'Winnie', 'Yesenia', 'Yolanda', 'Young', 'Yvette', 'Yvonne', 'Zachary', 'Zelma']
    lLastNames = [ \
        'Abbott', 'Acevedo', 'Acosta', 'Adams', 'Adkins', 'Aguilar', 'Aguirre', 'Alexander', 'Ali', 'Allen', 'Allison', 'Alvarado', 'Alvarez', 'Anderson', 'Andrade', 'Andrews', 'Anthony', 'Arellano', 'Arias', 'Armstrong', 'Arnold', 'Arroyo', 'Ashley', 'Atkins', 'Atkinson', 'Austin', 'Avery', 'Avila', 'Ayala', 'Ayers', 'Bailey', 'Baker', 'Baldwin', 'Ball', 'Ballard', 'Banks', 'Barber', 'Barker', 'Barnes', 'Barnett', 'Barr', 'Barrera', 'Barrett', 'Barron', 'Barry', 'Bartlett', 'Barton', 'Bass', 'Bates', 'Bauer', 'Bautista', 'Baxter', 'Bean', 'Beard', 'Beasley', 'Beck', 'Becker', 'Bell', 'Beltran', 'Bender', 'Benjamin', 'Bennett', 'Benson', 'Bentley', 'Benton', 'Berg', 'Berger', 'Bernard', 'Berry', 'Best', 'Bishop', 'Black', 'Blackburn', 'Blackwell', 'Blair', 'Blake', 'Blanchard', 'Blankenship', 'Blevins', 'Bond', 'Bonilla', 'Booker', 'Boone', 'Booth', 'Bowen', 'Bowers', 'Bowman', 'Boyd', 'Boyer', 'Boyle', 'Bradford', 'Bradley', 'Bradshaw', 'Brady', 'Brandt', 'Bray', 'Brennan', 'Brewer', 'Bridges', 'Briggs', 'Brock', 'Brooks', 'Brown', 'Browning', 'Bruce', 'Bryan', 'Bryant', 'Buchanan', 'Buck', 'Buckley', 'Bullock', 'Burch', 'Burgess', 'Burke', 'Burnett', 'Burns', 'Burton', 'Bush', 'Butler', 'Byrd', 'Cabrera', 'Cain', 'Calderon', 'Caldwell', 'Calhoun', 'Callahan', 'Camacho', 'Cameron', 'Campbell', 'Campos', 'Cannon', 'Cantrell', 'Cantu', 'Cardenas', 'Carey', 'Carlson', 'Carpenter', 'Carr', 'Carrillo', 'Carroll', 'Carson', 'Carter', 'Case', 'Casey', 'Castaneda', 'Castillo', 'Castro', 'Cervantes', 'Chambers', 'Chan', 'Chandler', 'Chang', 'Chapman', 'Charles', 'Chase', 'Chavez', 'Chen', \
        'Cherry', 'Choi', 'Christensen', 'Christian', 'Chung', 'Church', 'Cisneros', 'Clark', 'Clarke', 'Clay', 'Clayton', 'Clements', 'Cline', 'Cobb', 'Cochran', 'Coffey', 'Cohen', 'Cole', 'Coleman', 'Collier', 'Collins', 'Colon', 'Combs', 'Compton', 'Conley', 'Conner', 'Conrad', 'Contreras', 'Conway', 'Cook', 'Cooper', 'Copeland', 'Cordova', 'Cortez', 'Costa', 'Cox', 'Craig', 'Crane', 'Crawford', 'Crosby', 'Cross', 'Cruz', 'Cummings', 'Cunningham', 'Curry', 'Curtis', 'Dalton', 'Daniel', 'Daniels', 'Daugherty', 'Davenport', 'David', 'Davidson', 'Davila', 'Davis', 'Dawson', 'Day', 'Dean', 'Decker', 'Delacruz', 'Deleon', 'Delgado', 'Dennis', 'Diaz', 'Dickerson', 'Dickson', 'Dillon', 'Dixon', 'Dodson', 'Dominguez', 'Donaldson', 'Donovan', 'Dorsey', 'Dougherty', 'Douglas', 'Doyle', 'Drake', 'Dudley', 'Duffy', 'Duncan', 'Dunlap', 'Dunn', 'Duran', 'Durham', 'Dyer', 'Eaton', 'Edwards', 'Elliott', 'Ellis', 'Ellison', 'English', 'Erickson', 'Escobar', 'Espinoza', 'Estes', 'Estrada', 'Evans', 'Everett', 'Farley', 'Farmer', 'Farrell', 'Faulkner', 'Ferguson', 'Fernandez', 'Fields', 'Figueroa', 'Finley', 'Fischer', 'Fisher', 'Fitzgerald', 'Fitzpatrick', 'Fleming', 'Fletcher', 'Flores', 'Flowers', 'Floyd', 'Flynn', 'Foley', 'Ford', 'Foster', 'Fowler', 'Fox', 'Francis', 'Franco', 'Frank', 'Franklin', 'Frazier', 'Frederick', 'Freeman', 'French', 'Friedman', 'Frost', 'Fry', 'Frye', 'Fuentes', 'Fuller', 'Gaines', 'Gallagher', 'Gallegos', 'Galvan', 'Garcia', 'Gardner', 'Garner', 'Garrett', 'Garrison', 'Garza', 'Gates', 'Gentry', 'George', 'Gibbs', 'Gibson', 'Gilbert', 'Giles', 'Gill', 'Gillespie', \
        'Gilmore', 'Glass', 'Glenn', 'Glover', 'Golden', 'Gomez', 'Gonzales', 'Gonzalez', 'Goodman', 'Goodwin', 'Gordon', 'Gould', 'Graham', 'Grant', 'Graves', 'Gray', 'Green', 'Greene', 'Greer', 'Gregory', 'Griffin', 'Griffith', 'Grimes', 'Gross', 'Guerra', 'Guerrero', 'Gutierrez', 'Guzman', 'Hahn', 'Hale', 'Haley', 'Hall', 'Hamilton', 'Hammond', 'Hampton', 'Hancock', 'Hanna', 'Hansen', 'Hanson', 'Hardin', 'Harding', 'Hardy', 'Harmon', 'Harper', 'Harrell', 'Harrington', 'Harris', 'Harrison', 'Hart', 'Hartman', 'Harvey', 'Hawkins', 'Hayden', 'Hayes', 'Haynes', 'Heath', 'Hebert', 'Henderson', 'Hendricks', 'Henry', 'Hensley', 'Henson', 'Herman', 'Hernandez', 'Herrera', 'Herring', 'Hess', 'Hester', 'Hickman', 'Hicks', 'Higgins', 'Hill', 'Hines', 'Ho', 'Hobbs', 'Hodge', 'Hodges', 'Hoffman', 'Hogan', 'Holland', 'Holloway', 'Holmes', 'Holt', 'Hood', 'Hoover', 'Hopkins', 'Horn', 'Horne', 'Horton', 'House', 'Houston', 'Howard', 'Howe', 'Howell', 'Huang', 'Hubbard', 'Huber', 'Hudson', 'Huff', 'Huffman', 'Hughes', 'Hull', 'Humphrey', 'Hunt', 'Hunter', 'Hurley', 'Hurst', 'Hutchinson', 'Huynh', 'Ibarra', 'Ingram', 'Jackson', 'Jacobs', 'Jacobson', 'James', 'Jefferson', 'Jenkins', 'Jennings', 'Jensen', 'Jimenez', 'Johns', 'Johnson', 'Johnston', 'Jones', 'Jordan', 'Joseph', 'Juarez', 'Kane', 'Kaufman', 'Keith', 'Keller', 'Kelley', 'Kelly', 'Kemp', 'Kennedy', 'Kent', 'Kerr', 'Khan', 'Kim', 'King', 'Kirby', 'Kirk', 'Klein', 'Kline', 'Knapp', 'Knight', 'Knox', 'Koch', 'Kramer', 'Krueger', 'Lam', 'Lamb', 'Lambert', 'Landry', 'Lane', 'Lang', 'Lara', 'Larsen', 'Larson', 'Lawrence', 'Lawson', 'Le', \
        'Leach', 'Leblanc', 'Lee', 'Leon', 'Leonard', 'Lester', 'Levine', 'Levy', 'Lewis', 'Li', 'Lin', 'Lindsey', 'Little', 'Liu', 'Livingston', 'Lloyd', 'Logan', 'Long', 'Lopez', 'Love', 'Lowe', 'Lowery', 'Lozano', 'Lucas', 'Luna', 'Lynch', 'Lynn', 'Lyons', 'Macdonald', 'Macias', 'Mack', 'Madden', 'Maddox', 'Mahoney', 'Maldonado', 'Malone', 'Mann', 'Manning', 'Marks', 'Marquez', 'Marsh', 'Marshall', 'Martin', 'Martinez', 'Mason', 'Massey', 'Mata', 'Mathews', 'Mathis', 'Matthews', 'Maxwell', 'May', 'Mayer', 'Maynard', 'Mays', 'Mcbride', 'Mccall', 'Mccann', 'Mccarthy', 'Mccarty', 'Mcclain', 'Mcclure', 'Mcconnell', 'Mccormick', 'Mccoy', 'Mccullough', 'Mcdaniel', 'Mcdonald', 'Mcdowell', 'Mcfarland', 'Mcgee', 'Mcguire', 'Mcintosh', 'Mcintyre', 'Mckay', 'Mckee', 'Mckenzie', 'Mckinney', 'Mclaughlin', 'Mclean', 'Mcmahon', 'Mcmillan', 'Mcpherson', 'Meadows', 'Medina', 'Mejia', 'Melendez', 'Melton', 'Mendez', 'Mendoza', 'Mercado', 'Merritt', 'Meyer', 'Meyers', 'Meza', 'Michael', 'Middleton', 'Miles', 'Miller', 'Mills', 'Miranda', 'Mitchell', 'Molina', 'Monroe', 'Montes', 'Montgomery', 'Montoya', 'Moody', 'Moon', 'Moore', 'Mora', 'Morales', 'Moran', 'Moreno', 'Morgan', 'Morris', 'Morrison', 'Morrow', 'Morse', 'Morton', 'Moses', 'Mosley', 'Moss', 'Moyer', 'Mueller', 'Mullen', 'Mullins', 'Munoz', 'Murphy', 'Murray', 'Myers', 'Nash', 'Navarro', 'Neal', 'Nelson', 'Newman', 'Newton', 'Nguyen', 'Nichols', 'Nicholson', 'Nielsen', 'Nixon', 'Noble', 'Nolan', 'Norman', 'Norris', 'Norton', 'Novak', 'Nunez', 'Obrien', 'Ochoa', 'Oconnell', 'Oconnor', 'Odonnell', 'Oliver', 'Olsen', 'Olson', 'Oneal', \
        'Oneill', 'Orozco', 'Orr', 'Ortega', 'Ortiz', 'Osborne', 'Owen', 'Owens', 'Pace', 'Pacheco', 'Padilla', 'Page', 'Palmer', 'Park', 'Parker', 'Parks', 'Parrish', 'Parsons', 'Patel', 'Patrick', 'Patterson', 'Patton', 'Paul', 'Payne', 'Pearson', 'Peck', 'Pena', 'Pennington', 'Perez', 'Perkins', 'Perry', 'Peters', 'Petersen', 'Peterson', 'Pham', 'Phelps', 'Phillips', 'Pierce', 'Pineda', 'Pittman', 'Pitts', 'Ponce', 'Poole', 'Pope', 'Porter', 'Potter', 'Potts', 'Powell', 'Powers', 'Pratt', 'Preston', 'Price', 'Prince', 'Proctor', 'Pruitt', 'Pugh', 'Quinn', 'Ramirez', 'Ramos', 'Ramsey', 'Randall', 'Randolph', 'Rangel', 'Rasmussen', 'Ray', 'Raymond', 'Reed', 'Reese', 'Reeves', 'Reid', 'Reilly', 'Reyes', 'Reynolds', 'Rhodes', 'Rice', 'Rich', 'Richard', 'Richards', 'Richardson', 'Richmond', 'Riley', 'Rios', 'Rivas', 'Rivera', 'Rivers', 'Roach', 'Robbins', 'Roberson', 'Roberts', 'Robertson', 'Robinson', 'Robles', 'Rocha', 'Rodgers', 'Rodriguez', 'Rogers', 'Rojas', 'Roman', 'Romero', 'Rosales', 'Rosario', 'Rose', 'Ross', 'Roth', 'Rowe', 'Rowland', 'Roy', 'Rubio', 'Ruiz', 'Rush', 'Russell', 'Russo', 'Ryan', 'Salas', 'Salazar', 'Salinas', 'Sampson', 'Sanchez', 'Sanders', 'Sandoval', 'Sanford', 'Santana', 'Santiago', 'Santos', 'Saunders', 'Savage', 'Sawyer', 'Schaefer', 'Schmidt', 'Schmitt', 'Schneider', 'Schroeder', 'Schultz', 'Schwartz', 'Scott', 'Sellers', 'Serrano', 'Sexton', 'Shaffer', 'Shah', 'Shannon', 'Sharp', 'Shaw', 'Shelton', 'Shepard', 'Shepherd', 'Sheppard', 'Sherman', 'Shields', 'Short', 'Silva', 'Simmons', 'Simon', 'Simpson', 'Sims', 'Singh', 'Singleton', 'Skinner', 'Sloan', \
        'Small', 'Smith', 'Snow', 'Snyder', 'Solis', 'Solomon', 'Sosa', 'Soto', 'Sparks', 'Spears', 'Spence', 'Spencer', 'Stafford', 'Stanley', 'Stanton', 'Stark', 'Steele', 'Stein', 'Stephens', 'Stephenson', 'Stevens', 'Stevenson', 'Stewart', 'Stokes', 'Stone', 'Stout', 'Strickland', 'Strong', 'Stuart', 'Suarez', 'Sullivan', 'Summers', 'Sutton', 'Swanson', 'Sweeney', 'Tanner', 'Tapia', 'Tate', 'Taylor', 'Terry', 'Thomas', 'Thompson', 'Thornton', 'Todd', 'Torres', 'Townsend', 'Tran', 'Trevino', 'Trujillo', 'Tucker', 'Turner', 'Tyler', 'Underwood', 'Valdez', 'Valencia', 'Valentine', 'Valenzuela', 'Vance', 'Vargas', 'Vasquez', 'Vaughan', 'Vaughn', 'Vazquez', 'Vega', 'Velasquez', 'Velazquez', 'Velez', 'Villa', 'Villanueva', 'Villarreal', 'Villegas', 'Vincent', 'Wade', 'Wagner', 'Walker', 'Wall', 'Wallace', 'Waller', 'Walls', 'Walsh', 'Walter', 'Walters', 'Walton', 'Wang', 'Ward', 'Ware', 'Warner', 'Warren', 'Washington', 'Waters', 'Watkins', 'Watson', 'Watts', 'Weaver', 'Webb', 'Weber', 'Webster', 'Weeks', 'Weiss', 'Welch', 'Wells', 'West', 'Wheeler', 'Whitaker', 'White', 'Whitehead', 'Wiggins', 'Wilcox', 'Wiley', 'Wilkerson', 'Wilkins', 'Wilkinson', 'Williams', 'Williamson', 'Willis', 'Wilson', 'Winters', 'Wise', 'Wolf', 'Wolfe', 'Wong', 'Wood', 'Woodard', 'Woods', 'Woodward', 'Wright', 'Wu', 'Wyatt', 'Yang', 'Yates', 'Yoder', 'York', 'Young', 'Yu', 'Zamora', 'Zimmerman', 'Zuniga']
    lOccupations = [ \
        'academic adviser', 'teacher', 'accompanist', 'music teacher', 'accountant', 'accountant-controller', 'accreditation officer', 'actor', 'ad writer', 'acoustical engineer', 'actuarial analyst', \
        'administrative judge', 'database administrator', 'nurse', 'advertising analyst', 'aerodynamics engineer', 'agent', 'agricultural scientist', 'agrochemist', \
        'agronomy engineer', 'aircraft design engineer', 'anchorperson', 'anesthetist', 'anthropologist', 'archaeologist', 'architect', 'archivist', \
        'art critic', 'artist', 'astronomer', 'athlete', 'attorney', 'author', 'bacteriologist', 'ballet dancer', 'belly dancer', 'biographer', 'biologist', \
        'biometrician', 'blues singer', 'botanist', 'broker', 'cardiac surgeon', 'cellist', 'certified general accountant', 'chancellor', 'chiropractor', \
        'cinematographer', 'climatologist', 'comedian', 'communications adviser', 'community planner', 'composer', 'conciliator', 'fitness consultant', \
        'corporation lawyer', 'cosmologist', 'criminologist', 'cryptanalyst', 'curator', 'cytogeneticist', 'dentist', 'dermatologist', 'dietitian', \
        'diplomat', 'drummer', 'ear specialist', 'ecologist', 'economist', 'endocrinologist', 'epidemiologist', 'essayist', 'etymologist', \
        'financial manager', 'fine arts professor', 'flutist', 'fossil conservator', 'gambling addictions counsellor', 'geographer', 'geriatrician', \
        'gynecologist', 'hematologist', 'historian', 'humorist', 'iconographer', 'improviser', 'journalist', 'landscape planner', 'legal advisor', \
        'linguist', 'marine engineer', 'mathematician', 'mediator', 'meteorologist', 'micropaleontologist', 'mining engineer', 'music director', \
        'naturopath', 'neurosurgeon', 'notary', 'novelist', 'obstetrician', 'oceanographer', 'osteopath', 'parasitologist', 'patent lawyer', \
        'pediatrician', 'pharmacist', 'preacher', 'radiologist', 'realtor', 'reporter', 'researcher', 'rheumatologist', 'salesman', \
        'social worker', 'songwriter', 'street musician', 'systems consultant', 'tax expert', 'toxicologist', 'translator', 'union adviser',
        'urologist', 'veterinarian', 'volcanologist', 'wood chemist', 'youth court judge', 'zoologist']
    lPostal1 = [("%s%d%s" % (random.choice(string.letters), random.randrange(1, 9), random.choice(string.letters))).upper() for i in xrange(100)]
    lPostal2 = [("%d%s%d" % (random.randrange(1, 9), random.choice(string.letters), random.randrange(1, 9))).upper() for i in xrange(400)]
    lAffinity.startTx()
    print "  progress [  0%%]",; sys.stdout.flush()
    lPINs = []
    def _processPIN(_pPIN):
        if False:
            _pPIN.savePIN()
        else:
            lPINs.append(_pPIN)
    for i in xrange(100):
        for j in xrange(100):
            print "\b\b\b\b\b\b%3d%%]" % int(100.0 * (i * j) / 10000),; sys.stdout.flush()
            _processPIN(PIN({"http://localhost/afy/property/testnotifications2/name":"%s, %s" % (random.choice(lLastNames), random.choice(lFirstNames)), \
                "http://localhost/afy/property/testnotifications2/occupation":random.choice(lOccupations), \
                "http://localhost/afy/property/testnotifications2/postalcode":("%s %s" % (random.choice(lPostal1), random.choice(lPostal2))), \
                "http://localhost/afy/property/testnotifications2/age":random.randrange(12,100)}))
        if len(lPINs) > 0:
            PIN.savePINs(lPINs)
            del lPINs[:]
    lAffinity.commitTx()

    lLocLetter = random.choice(string.letters).upper()
    lCntLoc = lAffinity.qCount("SELECT * FROM tn2c:Location('%s');" % lLocLetter)
    print ("%d instances found with a postal-code starting with '%s'" % (lCntLoc, lLocLetter))
    lAge = random.randrange(12,100)
    lCntAge = lAffinity.qCount("SELECT * FROM tn2c:Age(%s);" % lAge)
    print ("%d instances found with age %s" % (lCntAge, lAge))
    lOccLetter = random.choice(string.letters).lower()
    lCntOcc = lAffinity.qCount("SELECT * FROM tn2c:Occupation('%s');" % lOccLetter)
    print ("%d instances found with an occupation starting with '%s'" % (lCntOcc, lOccLetter))

    def intersectSelect():
        "For testing purposes, exercise both of these equivalent forms."
        #return ('&', 'INTERSECT SELECT * FROM')[random.choice([False, True])]
        return '&'
    lIJ = intersectSelect()
    lCntI1 = lAffinity.qCount("SELECT * FROM tn2c:Location('%s') %s tn2c:Age(%s) %s tn2c:Occupation('%s');" % (lLocLetter, lIJ, lAge, lIJ, lOccLetter))
    print ("%d instances found corresponding to postal-code(%s) age(%s) occupation(%s)" % (lCntI1, lLocLetter, lAge, lOccLetter))
    lCntI2 = lAffinity.qCount("SELECT * FROM tn2c:Location('%s') %s tn2c:Age(%s);" % (lLocLetter, intersectSelect(), lAge))
    print ("%d instances found corresponding to postal-code(%s) age(%s)" % (lCntI2, lLocLetter, lAge))
    lCntI4 = lAffinity.qCount("SELECT * FROM tn2c:Age(%s) %s tn2c:Occupation('%s');" % (lAge, intersectSelect(), lOccLetter))
    print ("%d instances found corresponding to age(%s) occupation(%s)" % (lCntI4, lAge, lOccLetter))
    
    AFYNOTIFIER.open(lAffinity)

    lNumNotifs = [0]
    def onClassNotif(pData, pCriterion):
        lNumNotifs[0] = lNumNotifs[0] + len(pCriterion)
        # Simulate delays in the handling of notifications (should have an aggregating effect).
        if random.choice([False, True]):
            time.sleep(random.choice([1, 2, 3]))
    def waitForNotifs(pMaxWaitInS, pExpectedNotifCount):
        print "waiting for notifications...",; sys.stdout.flush()
        for iW in xrange(pMaxWaitInS):
            if lNumNotifs[0] >= pExpectedNotifCount:
                break
            print ".",; sys.stdout.flush()
            time.sleep(1)
        time.sleep(2) # See if we catch more, unexpected notifications...
        print ("\nobtained %s notifications, expected %s" % (lNumNotifs[0], pExpectedNotifCount))
        assert lNumNotifs[0] == pExpectedNotifCount

    AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications2/Location", onClassNotif, pGroupNotifs=True)
    AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications2/Age", onClassNotif, pGroupNotifs=True)
    AFYNOTIFIER.registerClass("http://localhost/afy/class/testnotifications2/Occupation", onClassNotif, pGroupNotifs=True)

    print ("3. Unclassify instances of age(%s) that intersect with postal-code(%s)" % (lAge, lLocLetter))
    lNumNotifs[0] = 0    
    lCandidates = PIN.loadPINs(lAffinity.qProto("SELECT * FROM \"http://localhost/afy/class/testnotifications2/Location\"('%s') %s \"http://localhost/afy/class/testnotifications2/Age\"(%s);" % (lLocLetter, intersectSelect(), lAge)))
    for iC in lCandidates:
        del iC["http://localhost/afy/property/testnotifications2/age"]
    waitForNotifs(10, 3 * lCntI2)

    # lAffinity.q("SET PREFIX tn2c: 'http://localhost/afy/class/testnotifications2/';") # Note: the protobuf request resets the connection...
    lCntI3 = lAffinity.qCount("SELECT * FROM tn2c:Location('%s') %s tn2c:Occupation('%s');" % (lLocLetter, intersectSelect(), lOccLetter))
    print ("%d instances found corresponding to postal-code(%s) occupation(%s)" % (lCntI3, lLocLetter, lOccLetter))

    print ("4. Unclassify instances of occupation(%s) that intersect with postal-code(%s)" % (lOccLetter, lLocLetter))
    lNumNotifs[0] = 0
    lCandidates = PIN.loadPINs(lAffinity.qProto("SELECT * FROM \"http://localhost/afy/class/testnotifications2/Location\"('%s') %s \"http://localhost/afy/class/testnotifications2/Occupation\"('%s');" % (lLocLetter, intersectSelect(), lOccLetter)))
    for iC in lCandidates:
        del iC["http://localhost/afy/property/testnotifications2/occupation"]
    waitForNotifs(10, 3 * lCntI3)

    print ("5. Change instances of postal-code(%s)" % lLocLetter)
    lNumNotifs[0] = 0
    lCandidates = PIN.loadPINs(lAffinity.qProto("SELECT * FROM \"http://localhost/afy/class/testnotifications2/Location\"('%s');" % lLocLetter))
    for iC in lCandidates:
        iC["http://localhost/afy/property/testnotifications2/postalcode"] = "%s" % iC["http://localhost/afy/property/testnotifications2/postalcode"].lower()
    waitForNotifs(10, 3 * lCntLoc - lCntI3 - lCntI2)

    AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications2/Location", onClassNotif)
    AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications2/Age", onClassNotif)
    AFYNOTIFIER.unregisterClass("http://localhost/afy/class/testnotifications2/Occupation", onClassNotif)

    AFYNOTIFIER.close()
    lAffinity.close()

class TestNotifications2(AffinityTest):
    "A test for notifications emphasizing high number of notifications."
    def execute(self):
        _entryPoint()
AffinityTest.declare(TestNotifications2)

if __name__ == '__main__':
    lT = TestNotifications2()
    lT.execute()
