const fs = require('fs'); 
const csv = require('csv-parser');
const _ = require('lodash');
const PNF = require('google-libphonenumber').PhoneNumberFormat;

let inputFilePath = 'input.txt'
let rows = [];
const phoneUtil = require('google-libphonenumber').PhoneNumberUtil.getInstance();

fs.createReadStream(inputFilePath)
  .pipe(csv({ headers: false }))
  .on('data', (row) => rows.push(row))
  .on('end', () => {

    // Separate Headers
    const headers = _.first(rows);
    rows.shift();

    // Find columns with the same name
    let equals_name = [];
    for(let i of Object.keys(headers)){
      let eq = [i];
      for(let j of Object.keys(headers)){
        if(i != j && headers[i] == headers[j]){
          eq.push(j);
          delete headers[j];
        }
      }
      if(eq.length > 1){
        equals_name.push(eq);
      }
    }

    // Join columns with the same name
    equals_name.forEach((eq) => {
      rows.forEach((row) => {
        let concat = [];
        eq.forEach((index) => {
          concat.push(row[index]);
          delete row[index];
        });
        row[ eq[0] ] = _.concat([], concat);
      });
    });

    // Substitute index for the header name
    rows.forEach((value) => {
      for (let key of Object.keys(headers)) {
        value[ headers[key] ] = value[key];
        delete value[key];
      }
    });

    // Find rows with same eid
    let equals_eid = [];
    rows.forEach((value, index) => {
      let curr_eid = value['eid'];
      let eq_eid = [index];
      rows.forEach((v, i) => {
        if(index < i && curr_eid == v['eid']){
          eq_eid.push(i);
        }
      });
      if( eq_eid.length > 1){
        equals_eid.push(eq_eid);
      }
    });

    // Join rows with same eid
    equals_eid.forEach((eq) => {
      let joined_rows = _.clone(rows[ eq[0] ]);
      delete rows[ eq[0] ];
      eq.shift();
      eq.forEach((index) => {
        for(let col of Object.keys(rows[index])){
          if(col.includes(' ')){
            let concat = [];
            concat.push(_.clone(joined_rows[col]));
            concat.push(rows[index][col]);
            joined_rows[col] = _.concat([], concat);
          }
        }
        delete rows[index];
      });
      rows.push(joined_rows);
    });
    
    let new_rows = [];
    rows.forEach((value) => {
      if(value != null){
        new_rows.push(value);
      }
    });
    rows = _.clone(new_rows);

    // Create tags
    rows.forEach((row) => {
      let addresses = [];
      for(let col of Object.keys(row)){
        if(col.includes(' ')){
          let address = {};
          let col_split = col.split(' ');
          let type = col_split[0];
          let tags = [];
          for(i = 1; i < col_split.length; i++){
            if(col_split[i].includes(',')){
              tags.push(col_split[i].replace(',', ''));
            } else {
              tags.push(col_split[i]);
            }
          }

          address['type'] = type;
          address['tags'] = tags;

          if(typeof(row[col]) == "object"){
            row[col].forEach((elem) => {
              if(type == 'email' && validEmail(elem)){
                address['address'] = elem;
                addresses.push(address);
              } else if(type == 'phone' && validPhone(elem)){
                address['address'] = formatPhone(elem);
                addresses.push(address);
              }
            });
          } else {
            if(type == 'email' && validEmail(row[col])){
              address['address'] = row[col];
              addresses.push(address);
            } else if(type == 'phone' && validPhone(row[col])){
              address['address'] = formatPhone(row[col]);
              addresses.push(address);
            }
          }
          delete row[col];
        }
      }
      row['addresses'] = addresses;
    });

    fs.writeFile("output.json", JSON.stringify(rows, null, 2), function(err) {
      if (err) {
          console.log(err);
      }
    });
  });

function validEmail(email){
  var re = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  return re.test(String(email).toLowerCase());
}

function validPhone(phone){
  if(phone && !containsAlpha(phone)){
    const number = phoneUtil.parseAndKeepRawInput(phone, 'BR');
    if(phoneUtil.isValidNumber(number)){
      return true;
    }
  }
  return false;
}

function formatPhone(phone) {
  const number = phoneUtil.parseAndKeepRawInput(phone, 'BR');
  let e164 = phoneUtil.format(number, PNF.E164);
  let formated_phone = e164.replace('+', '');
  return formated_phone;
}

function containsAlpha(str) {
  var code, i, len;

  for (i = 0, len = str.length; i < len; i++) {
    code = str.charCodeAt(i);
    if (!(code > 47 && code < 58) && // numeric (0-9)
        !(code > 64 && code < 91) && // upper alpha (A-Z)
        !(code > 96 && code < 123)) { // lower alpha (a-z)
      return false;
    }
  }
  return true;
}