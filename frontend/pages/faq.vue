<template>
    <v-layout justify-center align-center wrap>
        <v-flex xs12 md10 my-4>
            <h1 class="display-4 text-xs-center">FAQ</h1>
        </v-flex>
        <v-flex xs12 md10 my-3 py-3>
            <h2 class="display-3 text-xs-center">Bitcoin Basics</h2>
            <v-expansion-panel>
                <v-expansion-panel-content
                        v-for="(item,i) in first"
                        :key="i"
                >
                    <template v-slot:header>
                        <h4 class="display-1">{{ item.title }}</h4>
                    </template>
                    <v-card>
                        <v-card-text class="qa-text" v-html="item.text">
                        </v-card-text>
                    </v-card>
                </v-expansion-panel-content>
            </v-expansion-panel>
        </v-flex>
        <v-flex xs12 md10 my-3 py-3>
            <h2 class="display-3 text-xs-center">Electricity Consumption</h2>
            <v-expansion-panel>
                <v-expansion-panel-content
                        v-for="(item,i) in second"
                        :key="i"
                >
                    <template v-slot:header>
                        <h4 class="display-1">{{ item.title }}</h4>
                    </template>
                    <v-card>
                        <v-card-text class="qa-text" v-html="item.text">
                        </v-card-text>
                    </v-card>
                </v-expansion-panel-content>
            </v-expansion-panel>
        </v-flex>
        <v-flex xs12 md10 my-3 py-3>
            <h2 class="display-3 text-xs-center">Environmental Impact</h2>
            <v-expansion-panel>
                <v-expansion-panel-content
                        v-for="(item,i) in third"
                        :key="i"
                >
                    <template v-slot:header>
                        <h4 class="display-1">{{ item.title }}</h4>
                    </template>
                    <v-card>
                        <v-card-text class="qa-text" v-html="item.text">
                        </v-card-text>
                    </v-card>
                </v-expansion-panel-content>
            </v-expansion-panel>
        </v-flex>
        <v-flex xs12 md10 my-3 py-3>
            <h2 class="display-3 text-xs-center">Clearing Up Common Misconceptions</h2>
            <v-expansion-panel style="width: 100%">
                <v-expansion-panel-content
                        v-for="(item,i) in fourth"
                        :key="i"
                >
                    <template v-slot:header>
                        <h4 class="display-1">{{ item.title }}</h4>
                    </template>
                    <v-card>
                        <v-card-text class="qa-text" v-html="item.text">
                        </v-card-text>
                    </v-card>
                </v-expansion-panel-content>
            </v-expansion-panel>
        </v-flex>
    </v-layout>
</template>

<script>

    export default {
        name: 'methodology',
        data() {
            return {
                first: [
                    {
                        "title": "What is Bitcoin?",
                        "text": "<p><strong>Bitcoin</strong> is a software protocol and peer-to-peer (P2P) network that enables the digital transfer of value across borders without relying on trusted intermediaries. Bitcoin is an open and permissionless system: anyone can participate in the network, as well as send, store, and receive payments without having to ask anyone for permission</p><p>Bitcoin has its own, native cryptocurrency called <strong>bitcoin</strong> (BTC) which acts as the universal unit of value within the Bitcoin network. New bitcoins are issued, according to a transparent and predictable schedule, on average every 10 minutes through a process called <strong>mining</strong>. While the Bitcoin protocol specifies that a maximum of 21 million bitcoins will ever be created, it is worth mentioning that one bitcoin can be divided out to eight decimal places. This means that one bitcoin corresponds to 100 million <strong>satoshi</strong>, the smallest base unit.</p>"
                    },
                    {
                        "title": "What is the blockchain?",
                        "text": "<p>All bitcoin transactions are recorded in a public ledger that every network participant (node) stores locally. The ledger is represented using a particular data structure: transactions are bundled into data blocks, which are then cryptographically linked to each other. This process results in a growing chain of blocks &ndash; which is fittingly called the blockchain. The use of this specific data structure ensures that tampering with the transaction history (e.g. modifying past transactions) will be detected immediately by other network participants. The blockchain grows larger every day as new blocks of transactions keep getting added by special participants called <strong>miners.</strong></p>"
                    },
                    {
                        "title": "Why do we need decentralised consensus?",
                        "text": "<p>Imagine Alice has 10 bitcoins that she wants to send to Bob. Shortly after, she changes her mind and suddenly wants to send the same 10 bitcoins to Charlie instead. While both transactions are valid, only one can get processed — Alice only has 10 bitcoins, not 20! The question now is: in a decentralised system like Bitcoin that has no central authority, who gets to decide which of the two valid but conflicting transactions will get processed?</p>"
                    },
                    {
                        "title": "What is Proof-of-Work (PoW) mining?",
                        "text": "<p>PoW mining is a mechanism for resolving the aforementioned situation in a decentralised manner: instead of simply letting participants vote (and thus making the vote vulnerable to potential manipulation by attackers who can create multiple fake identities), the idea is to attach a financial cost to the vote. Anyone who wants to participate in the vote (<strong>miners</strong>) needs to prove that they performed some &ldquo;work&rdquo; &ndash; hence the term <strong>proof-of-work</strong>.&nbsp;</p><p>This work consists of finding the solution to a cryptographic puzzle, which in simplistic terms can be thought of as guessing a random number. The only way of finding the random number — called a <strong>nonce</strong> in technical jargon — is to brute force all possible options. This way, the work cannot be faked but is trivial to verify by other network participants once the winning nonce has been revealed.</p><p>Solving the PoW requires substantial computing power depending on the difficulty level: the more miners join the race, the more difficult the puzzle becomes. Rather than guessing numbers manually, miners operate specialised mining equipment (ASICs) that has been specifically designed to be very good at only one single task — solving the PoW.&nbsp;</p><p>Miners need to incur financial costs in the form of <strong>capital expenditures</strong> (acquiring mining hardware) as well as <strong>operational expenditures</strong> (spending electricity to run and cool the machines). Miners are in constant competition between themselves: whoever finds the solution to the puzzle first obtains the right to add his block to the global ledger. In return, the successful miner gets rewarded for his efforts with newly minted bitcoin.&nbsp;</p><p>In the event that two valid but competing blocks are found at roughly the same time by different miners, the chain will split into two branches. Network participants will always follow the &ldquo;longest&rdquo; chain, that is the branch that was most difficult to produce (i.e. required more computing power, and was thus more expensive to generate). The idea is that miners should have a <strong>financial incentive</strong> to play by the rules as they stand to gain more from being honest than if they were to cheat.</p><p>Or, in the words of anonymous Bitcoin creator Satoshi Nakamoto:</p><p><em>[A greedy attacker] ought to find it more profitable to play by the rules, such rules that favour him with more new coins than everyone else combined, than to undermine the system and the validity of his own wealth.</em><sup> 1</sup></p><p>&nbsp;</p><hr /><p><sup>1 </sup><sub>Satoshi Nakamoto (2008) <em>Bitcoin: A Peer-to-Peer Electronic Cash System.</em> Available at: <a href='https://bitcoin.org/bitcoin.pdf'target='_blank'>https://bitcoin.org/bitcoin.pdf</a></sub></p>"
                    }
                ],
                second: [
                    {
                        "title": "Why does Bitcoin consume electricity?",
                        "text": "<p>We mentioned before that PoW mining rests on the premise that a financial cost needs to be attached to the vote. It turns out that this cost primarily comes in the form of electricity that needs to be expended in order to run mining machines. And mining hardware consumes quite a lot of electricity, to say the least!</p><p>The more machines a miner operates, the more likely he is to find the solution to the puzzle. However, more machines also means that more electricity is needed to run and cool the equipment, which in turn results in higher costs for the miner in question. Miners are thus always searching for abundant electricity sources at the lowest possible price.</p><p>While newer ASIC models are substantially more energy-efficient than previous generations, they still consume a significant amount of electricity. Rising bitcoin prices make mining more attractive, as the potential reward increases in value. As a result, new mining hardware will get added to the network and lead to increasing electricity consumption overall.</p><p>The electricity consumption is thus closely linked to total mining revenues (block subsidy and transaction fees), which are a function of the Bitcoin price.</p>"
                    },
                    {
                        "title": "How much electricity does Bitcoin use?",
                        "text": "<p>It is not possible to exactly determine how much electricity Bitcoin uses for a variety of reasons.</p><p>For instance, miners can choose between several ASIC models that can have different energy efficiencies: there is little data available on the exact market share of mining hardware, and miners often reconnect old, less efficient machines when block rewards go up in value. It is also difficult to determine what hardware miners are using as profitability may vary significantly from one region to another because of different electricity costs.</p><p>Moreover, mining operations require additional electricity to cool the machines to prevent them from overheating, and eventually breaking. Data centres can vary significantly in terms of how efficiently they use electricity: while cooling and overheads for the most efficient facilities account for less than 2% of the electricity used to run the mining equipment, less efficient data centres can have significantly higher figures. It is not possible to exactly determine how efficiently mining facilities use electricity as they are located in different regions and have different configurations and settings.</p><p>The best option is thus to build theoretical models based on specific assumptions (see the <a href='https://cbeci.org/cbeci/methodology'target='_blank'>Methodology</a> section) in order to provide an educated guess. The CBECI also provides a range of potential estimates, delineated by a lower bound representing the absolute minimum consumption (floor) and an upper bound corresponding to the absolute maximum consumption (ceiling).</p>"
                    },
                    {
                        "title": "What is the difference between the two CBECI numbers?",
                        "text": "<p>The first number, expressed in gigawatts (GW) and updated every 30 seconds, corresponds to the <strong>electrical power</strong> of the Bitcoin network. Power is the rate at which electricity is produced or consumed.<sup>1</sup></p><p>The second number, expressed in terawatt-hours (TWh), corresponds to the total <strong>yearly electricity consumption</strong> of the Bitcoin network. It is an annualised measure that takes the previous power estimate and assumes continuous usage over an entire year. A 7-day moving average is applied in order to make the figure less dependent on short-term hashrate volatility and allow for better comparisons.</p><p>&nbsp;</p><hr /><p><sup>1 </sup><sub>This simplification is slightly inaccurate as energy is never destroyed, but instead changes forms (transformation).</sub></p>"
                    }
                ],
                third: [
                    {
                        "title": "What are Bitcoin’s carbon dioxide emissions?",
                        "text": "<p>It is important to understand that energy consumption is not necessarily equivalent to carbon dioxide emissions and environmental pollution. For instance, one kilowatt-hour (kWh) of electricity generated by a coal-fired power station has a substantially different environmental footprint than one kWh of electricity produced by a solar park.</p><p>In order to determine Bitcoin&rsquo;s carbon dioxide emissions, and thus its real environmental footprint, the actual <strong>energy mix</strong> (i.e. sources of energy used to produce electricity) needs to be examined more closely. While some mining facilities disclose the energy sources used to power their machines, the exact energy mix of the majority of mining farms remains unknown.</p><p>More recently, studies have shown that a growing share of total electricity consumption originates from renewable energy sources such as hydro, solar, and wind power. However, estimates are varying widely, ranging from approximately 20%<sup> 1</sup> of the total energy mix to more than 70%.<sup>2</sup></p><p>In a second phase of this project, we plan to launch an interactive geographic map that tracks the location and energy mix of Bitcoin mining facilities. This granular data enables a bottom-up analysis that would lead to a more accurate representation of Bitcoin&rsquo;s real environmental footprint.</p><p>&nbsp;</p><hr /><p><sup>1 </sup><sub>Cambridge Centre for Alternative Finance (2018) <em>2nd Global Cryptoasset Benchmarking Study</em>. Available at: <a href='https://www.jbs.cam.ac.uk/faculty-research/centres/alternative-finance/publications/2nd-global-cryptoasset-benchmark-study/' target='_blank'>https://www.jbs.cam.ac.uk/faculty-research/centres/alternative-finance/publications/2nd-global-cryptoasset-benchmark-study/</a></sub></p><p><sup>2 </sup><sub>Coinshares (2019) <em>The Bitcoin Mining Network</em>. Available at: <a href='https://coinshares.co.uk/wp-content/uploads/2019/06/MiningWhitepaperJun2019FinalForeword.pdf'target='_blank'>https://coinshares.co.uk/wp-content/uploads/2019/06/MiningWhitepaperJun2019FinalForeword.pdf</a></sub></p>"
                    },
                    {
                        "title": "Does this mean Bitcoin boils the oceans?",
                        "text": "<p>There is currently little evidence suggesting that Bitcoin directly contributes to climate change. Even when assuming that Bitcoin mining was exclusively powered by coal - a very unrealistic scenario given that a non-trivial number of facilities run exclusively on renewables - total carbon dioxide emissions would not exceed 58 million tons of CO<sub>2</sub><sup> 1</sup>, which would roughly correspond to 0.17% of the world&rsquo;s total emissions.<sup>2</sup></p><p>This is not to say that environmental concerns regarding Bitcoin&rsquo;s electricity consumption should be disregarded. There are valid concerns that Bitcoin&rsquo;s growing electricity consumption may pose a threat to achieving the United Nations Sustainable Development Goals in the future.</p><p>However, current figures should be put into perspective: available data shows that even in the worst case (i.e. mining exclusively powered by coal), Bitcoin&rsquo;s environmental footprint currently remains marginal at best.</p><p>&nbsp;</p><hr /><p><sup>1 </sup><sub>This calculation is based on the following assumptions:</sub></p><p style='padding-left: 30px;'><sub>- The Bitcoin network consumes 50 TWh of electricity on an annual basis;</sub><sub><br />- One kWh of electricity generated by brown coal produces 1.17kg of CO<sub>2</sub>.</sub></p><p><sup>2 </sup><sub>BP (2018) <em>BP Statistical Review of World Energy</em>. 67th Edition, p.49. Available at: <a href='https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2018-full-report.pdf' target='_blank' rel='noopener'>https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2018-full-report.pdf</a></sub></p>"
                    }
                ],
                fourth: [
                    {
                        "title": "“Bitcoin needs to become efficient”",
                        "text": "<p>An often misunderstood criticism of Bitcoin relates to the inefficiencies inherent in the system. However, in order to preserve its decentralised nature, and as a result its censorship resistance, Bitcoin precisely needs to be inefficient in order prevent a single entity or colluding group of actors to easily gain control and dominate the network.&nbsp;</p><p>These inefficiencies are manifested in a variety of ways: every fully-validating node needs to first download every single transaction and block, then verify every single transaction and block, and then store every single transaction and block forever. This massive duplication and redundancy is necessary to allow anyone to independently verify the state of the network and ledger without having to trust anyone else.</p><p>Similarly, Bitcoin&rsquo;s network security depends on PoW mining being expensive: if mining a block is sufficiently difficult &mdash; and thus costly in financial terms, malicious actors will find it much harder to successfully perform an ongoing attack on the blockchain (e.g. perform double-spends, mine empty blocks, or reorganise the blockchain and reverse old transactions). Doing so would force attackers to spend significant resources in the form of specialised mining equipment and electricity ex-ante.&nbsp;</p>"
                    },
                    {
                        "title": "The “energy cost per transaction”",
                        "text": "<p>The popular &ldquo;energy cost per transaction&rdquo; metric is regularly featured in the media and other academic studies despite having multiple issues.</p><p>First, transaction throughput (i.e. the number of transactions that the system can process) is independent of the network&rsquo;s electricity consumption. Adding more mining equipment and thus increasing electricity consumption will have no impact on the number of processed transactions.</p><p>Second, a single Bitcoin transaction can contain hidden semantics that may not be immediately visible nor intelligible to observers. For instance, one transaction can include hundreds of payments to individual addresses, settle second-layer network payments (e.g. opening and closing channels in the <a href='https://lightning.network/'target='_blank'>Lightning network</a>), or potentially represent billions of timestamped data points using open protocols such as <a href='https://opentimestamps.org/'target='_blank'>OpenTimestamps</a>.</p>"
                    },
                    {
                        "title": "Incomplete comparisons",
                        "text": "<p>Many electricity consumption estimates include comparisons with traditional payment systems. These may initially seem appropriate given that Bitcoin is often touted as global payment network. However, a closer look at the value proposition of these systems reveals substantial differences: unlike traditional payment systems, Bitcoin is designed to function as an open censorship-resistant value transfer system that anyone can access without requiring permission. Achieving these properties requires engaging in different trade-offs which, as mentioned previously, necessarily results in massive operational costs and inefficiencies.</p><p>Furthermore, Bitcoin is more than a mere payment system: its native cryptocurrency bitcoin can be considered a synthetic commodity money that may be used as a long-term store of value (bitcoin is frequently called &ldquo;digital gold&rdquo;) and/or a means of exchange for purchasing goods and services (&ldquo;digital cash&rdquo;). Thanks to its security, the Bitcoin network can also be used as a public notary to verify the existence and integrity of timestamped data (e.g. documents).</p>"
                    }
                ]
            }
        }
    }
</script>
