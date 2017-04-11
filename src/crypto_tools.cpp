
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#include <io.h>
#else
#include <unistd.h>
#endif
#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <fstream>
#include <cryptopp/cryptlib.h>
#include <cryptopp/osrng.h>
#include <cryptopp/base64.h>
#include <cryptopp/hex.h>
#include <cryptopp/filters.h>
#include <cryptopp/files.h>
#include <cryptopp/eccrypto.h>
#include <cryptopp/gfpcrypt.h>
#include <cryptopp/rsa.h>
#include <nlohmann/json.hpp>

void usage(const char* prog) {
  fprintf(stderr,
    R"EOS(Usage: %s Options
Options:
  -s <seed file>
  -k <private key>
  -p <public key>
     if empty , $(-k).pub
  -l <license file>
  -i <id>
  -b <begin time> UTC ms
  -d <duration> ms

  -G gen key pair
     require -skp
  -S sign a license
     require -libdk
  -V verify a license
     output success/timeout/fail
     require -lp
)EOS", prog);
  exit(1);
}

typedef CryptoPP::RSASS<CryptoPP::PKCS1v15, CryptoPP::SHA256> RsaWithSha256;

void gen(const std::string& seedFile, const std::string& privateKey, const std::string& publicKey) {
  using namespace CryptoPP;
  uint32_t keyLength = 2048;

  RandomPool pool;
  FileSource seed(seedFile.c_str(), true);
  SecByteBlock seedBuffer((size_t)seed.MaxRetrievable());
  seed.Get(seedBuffer, seedBuffer.size());
  pool.IncorporateEntropy(seedBuffer.begin(), seedBuffer.size());

  RSAES_OAEP_SHA_Decryptor priv(pool, keyLength);
  Base64Encoder privFile(new FileSink(privateKey.c_str()), true);
  priv.DEREncode(privFile);
  privFile.MessageEnd();

  RSAES_OAEP_SHA_Encryptor pub(priv);
  Base64Encoder pubFile(new FileSink(publicKey.c_str()), true);
  pub.DEREncode(pubFile);
  pubFile.MessageEnd();
}

void sign(const std::string& id, uint64_t beginDate, uint64_t duration, const std::string& signedFile, const std::string& privateKey) {
  using namespace CryptoPP;
  using json = nlohmann::json;
  FileSource privFile(privateKey.c_str(), true, new Base64Decoder);
  AutoSeededRandomPool pool;
  RsaWithSha256::Signer priv(privFile);

  json rawJson;
  rawJson["id"] = id;
  rawJson["dat"] = beginDate;
  rawJson["dur"] = duration;
  std::string rawMsg = rawJson.dump();
  StringSource msgSource(rawMsg, true, new Base64Encoder(nullptr, false));
  std::string msgBase64;
  msgBase64.resize(msgSource.MaxRetrievable());
  msgSource.Get((byte*)msgBase64.data(), msgBase64.size());

  StringSource signedSource(rawMsg, true, new SignerFilter(pool, priv, new Base64Encoder(nullptr, false)));
  std::string signedBase64;
  signedBase64.resize(signedSource.MaxRetrievable());
  signedSource.Get((byte*)signedBase64.data(), signedBase64.size());

  json signedJson;
  signedJson["data"] = msgBase64;
  signedJson["sign"] = signedBase64;
  std::ofstream(signedFile) << signedJson.dump(2);
}
void verify(const std::string& signedFile, const std::string& publicKey) {
  using namespace CryptoPP;
  using json = nlohmann::json;
  FileSource pubFile(publicKey.c_str(), true, new Base64Decoder);
  RsaWithSha256::Verifier pub(pubFile);
  
  json signedJson = json::parse(std::ifstream(signedFile, std::ios::binary));
  std::string strSign = signedJson["sign"].get<std::string>();
  StringSource signedSource(strSign, true, new Base64Decoder);
  if (signedSource.MaxRetrievable() != pub.SignatureLength())
  {
    fprintf(stdout, "fail");
    return;
  }
  SecByteBlock signature(pub.SignatureLength());
  signedSource.Get(signature, signature.size());

  VerifierFilter *verifierFilter = new VerifierFilter(pub);
  verifierFilter->Put(signature, pub.SignatureLength());
  std::string strData = signedJson["data"].get<std::string>();
  StringSource dataSource(strData, true, new Base64Decoder(verifierFilter));
  fprintf(stdout, verifierFilter->GetLastResult() ? "success" : "fail");
}

int main(int argc, char* argv[]) {
  int isGen = 0;
  int isSign = 0;
  int isVerify = 0;
  std::string seedFile;
  std::string privateKey;
  std::string publicKey;
  std::string licenseFile;
  std::string id;
  uint64_t beginDate = 0;
  uint64_t duration = 0;

  for (;;) {
    int opt = getopt(argc, argv, "s:k:p:l:i:b:d:GSV");
    switch (opt) {
    case -1:
      goto GO;
    default:
      usage(argv[0]);
    case 's':
      seedFile = optarg;
      break;
    case 'k':
      privateKey = optarg;
      break;
    case 'p':
      publicKey = optarg;
      break;
    case 'l':
      licenseFile = optarg;
      break;
    case 'i':
      id = optarg;
    case 'b':
      beginDate = std::atoll(optarg);
      break;
    case 'd':
      duration = std::atoll(optarg);
      break;
    case 'G':
      isGen = 1;
      break;
    case 'S':
      isSign = 1;
      break;
    case 'V':
      isVerify = 1;
      break;
    }
  }
GO:
  if (publicKey.empty()) {
    publicKey = privateKey + ".pub";
  }
  if (isGen + isSign + isVerify != 1) {
    usage(argv[0]);
    return -1;
  }
  if (isGen) {
    if (seedFile.empty() || privateKey.empty()) {
      usage(argv[0]);
      return -1;
    }
    gen(seedFile, privateKey, publicKey);
  }
  if (isSign) {
    if (id.empty() || licenseFile.empty() || privateKey.empty() || beginDate == 0 || duration == 0) {
      usage(argv[0]);
      return -1;
    }
    sign(id, beginDate, duration, licenseFile, privateKey);
  }
  if (isVerify) {
    if (licenseFile.empty() || publicKey.empty()) {
      usage(argv[0]);
      return -1;
    }
    verify(licenseFile, publicKey);
  }
  return 0;
}