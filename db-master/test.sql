create table author(id NUMBER, name TEXT)
create table book(id NUMBER, name TEXT, author_id NUMBER)
-- select * from author
-- select * from book order by id desc
-- select * from book join author on book.author_id = author.id

-- Path: db-master/author.sql
insert into author(id, name) values(1, 'John')
insert into author(id, name) values(2, 'Jane')
insert into author(id, name) values(3, 'Jack')
insert into author(id, name) values(4, 'Jill')
insert into author(id, name) values(5, 'Jenny')
insert into author(id, name) values(6, '六')

-- Path: db-master/book.sql
insert into book(id, name, author_id) values(1, 'Book 1', 1)
insert into book(id, name, author_id) values(2, 'Book 2', 1)
insert into book(id, name, author_id) values(3, 'Book 3', 2)
insert into book(id, name, author_id) values(4, 'Book 4', 2)
insert into book(id, name, author_id) values(5, 'Book 5', 3)
insert into book(id, name, author_id) values(6, 'Book 6', 3)
insert into book(id, name, author_id) values(7, 'Book 7', 4)
insert into book(id, name, author_id) values(8, 'Book 8', 4)
insert into book(id, name, author_id) values(9, 'Book 9', 5)
insert into book(id, name, author_id) values(10, 'Book 10', 5)
insert into book(id, name, author_id) values(11, 'Book 11', 6)

-- update book set name = 'Book 1' where author_id = 1

-- delete from book where id = 11

create table langbook(id NUMBER, name TEXT, info TEXT)
insert into langbook(id, name, info) values(1, '书1', 'Book 1 info')
insert into langbook(id, name, info) values(2, '书2', 'Book 2 info')
insert into langbook(id, name, info) values(3, 'ja', 'Book 3 info')
insert into langbook(id, name, info) values(4, 'ja', 'Book 4 info')
insert into langbook(id, name, info) values(5, 'book 1', 'Book 5 info')
insert into langbook(id, name, info) values(6, 'book 2', 'Book 6 info')
