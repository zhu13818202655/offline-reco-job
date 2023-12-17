/* global $Ctx */

const data = $Ctx.Data.yaml('data.yaml')

$Ctx.Spec = {
  template: {
    'uid|1': Object.keys(data.users),
    name: function () {
      return data.users[this.uid].name
    }
  }
}
