import os
from tools.dir_util import project_dir

# all tokens (351)
# tokens_list = ["POPCAT","SILLY","NKN","RATS","CAT","IQ","ORBS","BLZ","TAO","AUCTION","MYRO","LOOM",
#                "TRB","AIDOGE","ZEN","COTI","IOTA","XNO","BOND","REEF","LTO","SCA","ZKF","SLERF","LAI",
#                "OGN","KNC","JOE","KEY","LADYS","BEAM","METIS","SKL","C","ETHW","ALPACA","ALICE","MAV",
#                "FXS","FUN","VRA","WEN","OMNI","DAR","MEW","RIF","MDT","SPELL","BADGER","BOBA","TRU","REN",
#                "UNFI","FLM","SCRT","TOKEN","OG","DASH","WAXP","CVX","ILV","SAGA","NFP","MAVIA","GAS","RPL",
#                "BAL","GFT","ZEC","TNSR","BNT","EDU","PIXEL","XRD","AMB","COQ","CETUS","FITFI","FOXY","KAS","FORTH","VANRY","DATA","IOST","ORCA","IOTX","AI","OXT","DUSK","MERL","LUNA","ANT","ACE","BSV","GODS","CFX","DODO","CHR","BIGTIME","NTRN","PERP","ZK","CELR","ORN","RVN","SFP","ZEUS","BAKE","LINA","SUPER","GLMR","STG","TLM","AUDIO","HNT","ROSE","BAND","RLC","HOT","MANTA","BAT","BEL","SATS","PAXG","DEGEN","SLP","YGG","PORTAL","PHB","ICX","STARL","SXP","BNX","RSS","XEM","LQTY","KDA","AGI","GAL","PEPE","ENJ","NMR","STORJ","TWT","T","EGLD","ARB","RAD","CAKE","KSM","ONE","RON","MKR","ZIL","BONK","LEVER","CTSI","RNDR","VTHO","HFT","ALGO","ONDO","STRK","GTC","IMX","OMG","PENDLE","COMBO","SNX","FLOKI","RSR","SUN","YFI","WIF","JUP","GMX","KLAY","CKB","ANKR","VET","DYDX","LIT","AXL","MOBILE","MEME","XAI","ENS","XVS","OCEAN","GALA","CRV","DYM","BICO","LUNC","API","ALPHA","POLYX","INJ","HOOK","USTC","LDO","BOME","PEOPLE","QTUM","AR","INCH","GMT","JST","ZRX","OP","BRETT","ADA","HBAR","SHIB","DOT","LRC","JASMY","APT","MBOX","LOOKS","W","ACH","REQ","QNT","WAVES","MASK","TIA","JTO","ID","FTM","THETA","CHZ","FLOW","ETC","MAGIC","HIGH","NFT","CORE","MATIC","SAND","ARK","OM","ETH","GRT","COMP","SSV","AGLD","RUNE","LSK","APE","CRO","LTC","NEAR","ICP","SUSHI","XRP","FIL","XLM","KAVA","ATOM","HIFI","AAVE","MANA","XMR","FET","UNI","AGIX","ATA","LINK","BSW","XTZ","AVAX","EOS","STMX","MINA","SOL","DOGE","SNT","CTK","DENT","ORDI","XEC","BTC","REZ","LPT","ASTR","SWEAT","CTC","STX","ARKM","AKRO","ARPA","ALT","MNT","FIRE","WOO","SUI","PUNDU","TRX","SC","BB","IDEX","MYRIA","PYTH","NEO","USDC","XCN","MOVR","MTL","XVG","WLD","STPT","CELO","ONT","TURBO","FLR","SEI","AXS","RDNT","BCH","ETHFI","ENA","SAFE","ZETA","CVC","GNO","DGB","TON","FRONT","PROM","ONG","MBL","TOMI","BNB","CYBER","STEEM","BTT","POWR","BLUR","COS","GLM","AERGO","QI","RARE","DAO","UMA","VGX","CEEK","ZBCN","AEVO","COVAL"]

# haircut>0.3 (132)
tokens_list = ['ETHW','FXS','TNSR','MERL','PERP','ZK','RVN','STG','ROSE','MANTA','BAT','SATS','PORTAL','KDA','GAL','PEPE',
               'TWT','EGLD','ARB','CAKE','KSM','ONE','MKR','ZIL','BONK','RNDR','HFT','ALGO','ONDO','STRK','IMX','PENDLE',
               'SNX','FLOKI','YFI','JUP','GMX','KLAY','ANKR','DYDX','AXL','MEME','XAI','ENS','GALA','CRV','DYM','INJ','HOOK',
               'LDO','BOME','PEOPLE','AR','GMT','ZRX','OP','ADA','HBAR','SHIB','DOT','LRC','W','ACH','QNT','WAVES','MASK',
               'TIA','JTO','ID','FTM','THETA','CHZ','FLOW','ETC','MAGIC','CORE','MATIC','SAND','ETH','GRT','COMP','SSV',
               'AGLD','RUNE','APE','LTC','NEAR','ICP','SUSHI','XRP','FIL','XLM','KAVA','ATOM','AAVE','MANA','FET','UNI','AGIX',
               'LINK','XTZ','AVAX','EOS','MINA','SOL','DOGE','ORDI','BTC','STX','ARKM','ALT','MNT','WOO','SUI','TRX','PYTH',
               'USDC','WLD','CELO','FLR','SEI','AXS','RDNT','BCH','ETHFI','ENA','ZETA','TON','BNB','CYBER','BLUR','AEVO']

tokens_multiplier_dict = {
    1000: ["PEPE", "FLOKI", "BONK", "SHIB", "LUNC", "XEC", "TURBO", "BTT"],
    10000: ["SATS","LADYS", "WENUSDT", "COQ", "STARL", "NFT"],
    10000000: ["AIDOGE"]
}

okx_token_list = ['PRCL','NOT','MSN','LQTY','BAND','ZENT','PEPE','TURBO','AEVO','CEL','ETHFI','USTC','BIGTIME','ETHW','ORBS','SSV','KNC','STRK','GMX','LSK','XCH','BONK','CSPR','FLM','SATS','FITFI','PYTH','ZETA','IOST','RDNT','LDO','ZRX','LOOKS','UMA','FLOKI','CORE','RVN','LUNA','WAXP','GALA','RSR','AR','AIDOGE','ALGO','WLD','DYDX','RNDR','CELO','ONE','ZERO','RACA','W','LUNC','BSV','ID','JTO','API3','LRC','FET','JUP','FRONT','AGIX','SOL','FOXY','ETC','CHZ','ICP','BNB','TON','BAT','GMT','CRV','CFX','MKR','JOE','BAL','KLAY','ZEUS','ALPHA','YGG','BNT','EGLD','WIF','MEME','EOS','XLM','BONE','ZIL','OP','MERL','VRA','ETH','AAVE','BADGER','ARB','ZK','SHIB','FTM','APT','DOGE','TNSR','XTZ','BTC','GFT','ORDI','ACH','STORJ','VELO','MANA','MAGIC','GLM','ENS','WOO','GAS','INJ','MATIC','BICO','TRB','FIL','GPT','IOTA','MEW','METIS','SPELL','APE','CRO','CETUS','LPT','MINA','MASK','COMP','STX','OM','GRT','HBAR','SUSHI','NEO','THETA','XRP','SAND','FLOW','QTUM','SNX','ADA','NEAR','SUI','PEOPLE','ATOM','YFI','ICX','DOT','ETH','SWEAT','UNI','BTC','VENOM','LTC','GODS','KSM','ACE','ONT','AGLD','AVAX','REN','LINK','GAL','TRX','MOVR','IMX','PERP','TIA','RAY','NMR','FXS','T','DMAIL','NFT','ENJ','BLUR','1INCH','CTC','FLR','BCH','SLP','CVC','DGB','AXS','USDC','RON','OMG','KISHU','BLOCK','JST','AUCTION']

'''dir'''
data_dir = os.path.join(project_dir(), "data")