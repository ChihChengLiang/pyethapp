import pytest
from devp2p.service import BaseService
from ethereum.config import default_config
from pyethapp.config import update_config_with_defaults, get_default_config
from ethereum.slogging import get_logger, configure_logging
from ethereum.hybrid_casper import chain as hybrid_casper_chain
from ethereum.tools import tester
from ethereum.tests.hybrid_casper.testing_lang import TestLangHybrid
from ethereum.utils import encode_hex
from pyethapp.app import EthApp
from pyethapp.db_service import DBService
from pyethapp.accounts import Account, AccountsService
from pyethapp.validator_service import ValidatorState, ValidatorService

log = get_logger('tests.validator_service')
configure_logging('validator:debug,eth.chainservice:debug,eth.pb.tx:debug')

transaction_queue = []  # Probably best to replace with a real TransactionQueue()

class ChainServiceMock(BaseService):
    name = 'chain'

    def __init__(self, app, test):
        super(ChainServiceMock, self).__init__(app)

        class InnerNewHeadCbsMock(object):
            def __init__(self, outer):
                self.outer = outer

            def append(self, cb):
                self.outer.chain.new_head_cb = cb

        # Save as interface to tester
        self.test = test
        self.on_new_head_cbs = InnerNewHeadCbsMock(self)
        self.chain = hybrid_casper_chain.Chain(genesis=test.genesis)
        self.txs = set()
        self.is_syncing = False

    def broadcast_transaction(self, tx):
        # Relay transactions into the tester for mining
        self.txs.add(tx)
        transaction_queue.append(tx)
        print('Adding tx: {}'.format(tx))
        return

    # Override this classmethod and add another arg
    @classmethod
    def register_with_app(klass, app, test):
        s = klass(app, test)
        app.register_service(s)
        return s

class PeerManagerMock(BaseService):
    name = 'peermanager'

    def broadcast(*args, **kwargs):
        pass

@pytest.fixture()
def test():
    return TestLangHybrid(5, 5, 0.02, 0.002)

@pytest.fixture()
def test_app(request, tmpdir, test):
    config = {
        'data_dir': str(tmpdir),
        'db': {'implementation': 'EphemDB'},
        'eth': {
            'block': {  # reduced difficulty, increased gas limit, allocations to test accounts
                'GENESIS_DIFFICULTY': 1,
                'BLOCK_DIFF_FACTOR': 2,  # greater than difficulty, thus difficulty is constant
                'GENESIS_GAS_LIMIT': 3141592,
                'GENESIS_INITIAL_ALLOC': {
                    encode_hex(tester.accounts[0]): {'balance': 10**28},
                },
                # Casper FFG stuff
                'EPOCH_LENGTH': 10,
                'WITHDRAWAL_DELAY': 100,
                'BASE_INTEREST_FACTOR': 0.02,
                'BASE_PENALTY_FACTOR': 0.002,
            }
        },
        # 'genesis_data': {},
        'validate': [encode_hex(tester.accounts[0])],
        'deposit_size': 5000 * 10**18,
        'should_logout': False,
    }

    services = [
        DBService,
        PeerManagerMock,
        ValidatorService,
    ]
    update_config_with_defaults(config, get_default_config([EthApp] + services))
    update_config_with_defaults(config, {'eth': {'block': default_config}})
    app = EthApp(config)

    # Add AccountsService first and initialize with coinbase account
    AccountsService.register_with_app(app)
    app.services.accounts.add_account(Account.new('', tester.keys[0]), store=False)

    # Need to do this one manually too
    ChainServiceMock.register_with_app(app, test)

    for service in services:
        service.register_with_app(app)

    return app

def test_valcode_deployment_and_successful_deposit(test, test_app):
    """ Check that the validator's valcode & deposit txs are generated & valid """
    test_app.chain = test.t.chain = test_app.services.chain.chain
    validator = test_app.services.validator
    test.parse('B1')
    # Check that the valcode tx was generated
    assert len(transaction_queue) > 0
    test.t.direct_tx(transaction_queue.pop())
    test.parse('B1')
    # Check that the valcode contract is deployed
    assert test.t.chain.state.get_code(validator.valcode_addr)
    print('Valcode deployed')
    # Check that the deposit tx was generated
    assert len(transaction_queue) > 0
    test.t.direct_tx(transaction_queue.pop())
    test.parse('B1')
    # Check that the validator has been added to the validator list
    validator_index = test.casper.validator_indexes(validator.coinbase.address)
    assert validator_index > 0
    print('Validator logged in with index: {}'.format(validator_index))

def test_vote_after_logged_in(test, test_app):
    """ Check that the validator submits vote txs correctly """
    test_app.chain = test.t.chain = test_app.services.chain.chain
    test.parse('B J0 B B')
    # validator = test_app.services.validator
    print('These are the txs', transaction_queue)
    # Make sure we attempted to deploy valcode & deposit
    assert len(transaction_queue) == 2
    test.parse('B1')
    # Check that the vote tx was generated
    assert len(transaction_queue) == 3
    test.t.direct_tx(transaction_queue.pop())
    test.parse('B1')
    # Get info required to check if the vote went through
    current_epoch = test.casper.current_epoch()
    expected_source_epoch = test.casper.expected_source_epoch()
    deposit = test.deposit_size / test.casper.deposit_scale_factor(current_epoch)
    # Check that the vote was counted
    assert test.casper.votes__cur_dyn_votes(current_epoch, expected_source_epoch) == deposit
    print('Validator submitted proper vote')

def test_validator_logout_and_withdrawal(test, test_app):
    """ Check that the validator logs out properly """
    test_app.chain = test.t.chain = test_app.services.chain.chain
    validator = test_app.services.validator
    test.parse('B J0 B B')
    # validator = test_app.services.validator
    print('These are the txs', transaction_queue)
    # Make sure we attempted to deploy valcode & deposit
    assert len(transaction_queue) == 2
    # Now set current state to waiting for log out
    validator.current_state = ValidatorState.waiting_for_log_out
    test.parse('B1')
    # Check that the logout & vote tx were generated and our current state is waiting for logout
    assert len(transaction_queue) == 4
    assert validator.current_state == ValidatorState.waiting_for_log_out
    # Apply vote and then apply logout
    test.t.direct_tx(transaction_queue.pop(len(transaction_queue)-2))
    validator.last_logout_broadcast = 0
    test.parse('B1')
    test.t.direct_tx(transaction_queue.pop())
    test.parse('B1')
    assert validator.current_state == ValidatorState.waiting_for_log_out
    test.parse('B V0 B V0 B')
    assert validator.current_state == ValidatorState.waiting_for_withdrawable
    test.parse('B B B B B')
    assert validator.current_state == ValidatorState.waiting_for_withdrawn
    test.t.direct_tx(transaction_queue.pop())
    test.parse('B1')
    assert validator.current_state == ValidatorState.logged_out
    print('Successfully completed logout and withdrawl!')
