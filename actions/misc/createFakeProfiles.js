const faker = require("faker");
const fs = require("fs");

var wtStream = fs.createWriteStream("fakeProfiles-100000-5.json", {
  flags: "a", // 'a' means appending (old data will be preserved)
});

for (var i = 1; i <= 100000; i++) {
  let person = {
    mobilePhone: { number: faker.phone.phoneNumber("##########") },
    personalEmail: { address: faker.internet.exampleEmail() },
    person: {
      name: {
        firstName: faker.name.firstName(),
        lastName: faker.name.lastName(),
      },
    },
  };

  wtStream.write(`${JSON.stringify(person)}\n`);
}
wtStream.end(); // close string
