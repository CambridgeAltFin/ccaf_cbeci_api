<template>
    <v-flex xs12 md10 my-3 py-3>
        <h2 class="display-3 text-xs-center">Feedback</h2>
        <v-form ref="form" v-model="valid" lazy-validation>
            <v-flex>
                <p class="text-sm-left">We look forward to receiving feedback, comments, suggestions, and constructive criticism that will help us refine the methodology, add more content, and improve the overall index. Please use the form below to share your thoughts.</p>
            </v-flex>
            <v-text-field
                    v-model="name"
                    :rules="nameRules"
                    label="Name"
                    solo
                    required
            />
            <v-text-field
                    v-model="organisation"
                    label="Organisation"
                    solo
            />
            <v-text-field
                    v-model="email"
                    :rules="emailRules"
                    label="E-mail"
                    required
                    solo
            />
            <v-textarea
                    v-model="message"
                    :rules="messageRules"
                    label="Your message ..."
                    required
                    solo
            />

            <v-flex>
                <p class="text-sm-left"><strong>How we use your personal information:</strong></p>
                <p class="text-sm-left">Cambridge Judge Business School will use your personal information to reply to your enquiry only. Read more about how we handle your personal information and your rights under the data protection legislation <a href="https://www.information-compliance.admin.cam.ac.uk/data-protection/general-data" target="_blank">here</a>.</p>
            </v-flex>
            <v-checkbox
                    v-model="checkbox"
                    :rules="[v => !!v || 'Your consent is required to continue']"
                    label="I have read and understood the above statement and consent to my personal information being used as described."
                    required
            />
            <v-flex v-if="error">
                An error has occurred. Please reload the webpage and try again or directly contact ccaf@jbs.cam.ac.uk.
            </v-flex>
            <v-flex v-if="status">
                Your message has been sent successfully.
            </v-flex>
            <v-btn
                    v-else
                    :disabled="!valid"
                    :loading="loading"
                    color="#FFB81C"
                    @click="validate"
            >
                <b>Send</b>
            </v-btn>

        </v-form>
    </v-flex>
</template>

<script>
    export default {
        name: 'feedback',
        data() {
            return {
                valid: true,
                loading: false,
                name: '',
                nameRules: [
                    v => !!v || 'Name is required',
                    v => (v && v.length <= 30) || 'Name must be less than 30 characters'
                ],
                email: '',
                emailRules: [
                    v => !!v || 'E-mail is required',
                    v => /.+@.+/.test(v) || 'E-mail must be valid'
                ],
                messageRules: [
                    v => (v && v.length <= 360) || 'Message must be less than 360 characters'
                ],
                organisation: '',
                message: '',
                checkbox: false,
                status: false,
                error: false
            }
        },
        computed: {
        },
        methods: {
            validate () {
                if (this.$refs.form.validate()) {
                    const self = this
                    self.loading = true
                    this.$axios.post('/feedback', {
                        email: self.email,
                        message: self.message,
                        name: self.name,
                        organisation: self.organisation
                    })
                        .then(function (response) {
                            if(response.data.status === 'success') {
                                self.status = true
                              self.message = ''
                              self.name = ''
                              self.email = ''
                              self.organisation = ''
                              self.checkbox = false
                              self.reset()
                            }
                            if(response.data.status === 'fail') {
                                self.error = true
                            }
                            self.loading = false
                        })
                        .catch(function (error) {
                            self.loading = false
                            alert(error)
                        });
                }
            },
            reset () {
                this.$refs.form.reset()
            },
            resetValidation () {
                this.$refs.form.resetValidation()
            }
        }
    }
</script>
