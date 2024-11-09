import express from "express";
import bodyParser from "body-parser";
import pg from "pg";

const port=3000;
const app = express();
const db=new pg.Client({
   user: 'postgres',
   host: 'localhost',
   database: 'Aggre',
   password: "siddh2003",
   port: 5432,
});
db.connect();

 app.use(express.static("public"));
 app.use(bodyParser.urlencoded({extended:true}));

 app.get("/",async (req,res)=>{
   const content=await db.query("SELECT content FROM news_article");
   const title=await db.query("SELECT title FROM news_article");
   // const published_at=await db.query("SELECT published_at FROM news_article");
   const original_url=await db.query("SELECT original_url FROM news_article");
   const image_url=await db.query("SELECT image_url FROM news_article");
   const score=await db.query("SELECT score FROM news_article");
   // let pubdates=[];
   let urls=[];
   let titles=[];
   let contents=[];
   let image_urls=[];
   let scores=[];
   score.rows.forEach((sco)=>{scores.push(sco.score);});
   title.rows.forEach((tit)=>{titles.push(tit.title);});
   content.rows.forEach((con)=>{contents.push(con.content);});
   // published_at.rows.forEach((pub)=>{pubdates.push(pub.published_at);});
   original_url.rows.forEach((ur)=>{urls.push(ur.original_url);});
   image_url.rows.forEach((im)=>{image_urls.push(im.image_url);});
   console.log(content.rows);
   // console.log(published_at.rows);
   console.log(original_url.rows);
   console.log(title.rows);
   console.log(image_url.rows);
   // console.log(pubdates[1]);
   res.render("index.ejs",{contents:contents,titles:titles,urls:urls,image_urls:image_urls,scores:scores});
 });

 app.listen(port,()=>{
    console.log(`Server is running on port ${port}`);
 })

